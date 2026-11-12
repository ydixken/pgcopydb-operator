#!/usr/bin/env python3
import copy
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

SCRIPT = Path(__file__).with_name('native-term-receipt.py')
SPEC = importlib.util.spec_from_file_location('native_term_receipt', SCRIPT)
M = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(M)

DIGEST = 'sha256:' + 'a' * 64


def owner(kind, uid):
    return [{'kind': kind, 'uid': uid, 'controller': True}]


def worker(state, container='containerd://one'):
    return {'name': 'pgcopydb', 'containerID': container, 'imageID': 'ghcr.io/x/runner@' + DIGEST, 'restartCount': 0, 'state': state}


def receipt(case):
    status = {'nativeJobUID': 'job-1', 'nativePVCUID': 'pvc-1', 'jobName': 'm-run-1', 'attempts': 1, 'replication': {'endpos': '0/16B3748'}}
    m = {'metadata': {'uid': 'mig-1', 'finalizers': ['pgcopydb-operator.io/cleanup']}, 'spec': {'suspend': False}, 'status': status}
    job = {'metadata': {'uid': 'job-1', 'ownerReferences': owner('Migration', 'mig-1')}, 'spec': {'suspend': False}, 'status': {'active': 1}}
    pod = {'metadata': {'uid': 'pod-1', 'ownerReferences': owner('Job', 'job-1')}, 'status': {'phase': 'Running', 'containerStatuses': [worker({'running': {}})]}}
    pvc = {'metadata': {'uid': 'pvc-1', 'ownerReferences': owner('Migration', 'mig-1')}}
    stopped = copy.deepcopy(pod)
    stopped['status'] = {'phase': 'Failed', 'containerStatuses': [worker({'terminated': {'exitCode': 4, 'finishedAt': '2026-11-13T10:00:20Z'}})]}
    intent = copy.deepcopy(m)
    if case == 'suspend':
        intent['spec']['suspend'] = True
    else:
        intent['metadata']['deletionTimestamp'] = '2026-11-13T10:00:00Z'
    failed = copy.deepcopy(job)
    failed['status'] = {'failed': 1, 'conditions': [{'type': 'Failed', 'status': 'True'}]}
    suspended = copy.deepcopy(intent)
    suspended['status']['phase'] = 'Suspended'
    c = copy.deepcopy
    after = {'migration': c(suspended), 'job': c(failed), 'pod': c(stopped), 'pvc': c(pvc)} if case == 'suspend' else {'migration': None}
    foreign = [{'kind': 'Job', 'namespace': 'ns', 'name': 'other', 'uid': 'foreign-1', 'resourceVersion': '7'}]
    return {'schema': M.SCHEMA, 'case': case, 'runnerImage': 'ghcr.io/x/runner@' + DIGEST, 'requestedAt': '2026-11-13T10:00:00Z',
            'before': {'migration': c(m), 'job': c(job), 'pod': c(pod), 'pvc': c(pvc)}, 'terminated': {'migration': c(intent), 'job': c(job), 'pod': c(stopped)},
            'after': after, 'foreign': {'before': c(foreign), 'after': c(foreign)}}


def term(r):
    return r['terminated']['pod']['status']['containerStatuses'][0]


MUTATIONS = {
    'schema': lambda r: r.update(schema='v0'),
    'runner-digest': lambda r: r.update(runnerImage='ghcr.io/x/runner:latest'),
    'before-receipts': lambda r: [m['status'].update(nativeJobUID='job-2') for m in (r['before']['migration'], r['terminated']['migration'], r['after']['migration'])],
    'before-ownership': lambda r: r['before']['pod']['metadata'].update(ownerReferences=owner('Job', 'job-2')),
    'before-running': lambda r: r['before']['pod']['status']['containerStatuses'][0].update(restartCount=1),
    'before-image': lambda r: r['before']['pod']['status']['containerStatuses'][0].update(imageID='ghcr.io/x/runner@sha256:' + 'b' * 64),
    'before-intent': lambda r: r['before']['migration']['spec'].update(suspend=True),
    'term-identity': lambda r: r['terminated']['pod']['metadata'].update(uid='pod-2'),
    'term-same-container': lambda r: term(r).update(containerID='containerd://two'),
    'term-engine-exit': lambda r: term(r)['state']['terminated'].update(exitCode=143),
    'term-window': lambda r: term(r)['state']['terminated'].update(finishedAt='2026-11-13T10:05:00Z'),
    'term-receipts-retained': lambda r: r['terminated']['migration']['status']['replication'].update(endpos='0/0'),
    'term-follows-intent': lambda r: r['terminated']['migration']['spec'].update(suspend=False),
    'after-suspended-retained': lambda r: r['after']['migration']['status'].update(phase='Following'),
    'after-resources-retained': lambda r: r['after']['pvc']['metadata'].update(uid='pvc-2'),
    'after-not-deleting': lambda r: r['after']['job']['metadata'].update(deletionTimestamp='2026-11-13T10:01:00Z'),
    'after-job-retained-failed': lambda r: r['after']['job']['spec'].update(ttlSecondsAfterFinished=60),
    'after-same-container': lambda r: r['after']['pod']['status']['containerStatuses'][0].update(containerID='containerd://two'),
    'foreign-unchanged': lambda r: r['foreign']['after'][0].update(resourceVersion='8'),
}


class NativeTermReceiptTest(unittest.TestCase):
    def test_valid_receipts_pass(self):
        for case in ('suspend', 'delete'):
            self.assertEqual(M.check(receipt(case)), [], case)

    def test_each_check_refuses_its_mutation(self):
        for name, mutate in MUTATIONS.items():
            with self.subTest(name):
                r = receipt('suspend')
                mutate(r)
                self.assertEqual(M.check(r), [name])

    def test_natural_completion_and_kill_are_not_term(self):
        for code in (0, 137):
            r = receipt('suspend')
            term(r)['state']['terminated']['exitCode'] = code
            self.assertEqual(M.check(r), ['term-engine-exit'], code)

    def test_delete_requires_release_and_pending_finalizer(self):
        r = receipt('delete')
        r['after']['migration'] = r['terminated']['migration']
        self.assertEqual(M.check(r), ['after-released'])
        r = receipt('delete')
        r['terminated']['migration']['metadata']['finalizers'] = []
        self.assertEqual(M.check(r), ['term-follows-intent'])

    def test_empty_foreign_inventory_is_not_success(self):
        r = receipt('delete')
        r['foreign'] = {'before': [], 'after': []}
        self.assertEqual(M.check(r), ['foreign-unchanged'])

    def test_cli_exit_status(self):
        with tempfile.TemporaryDirectory() as tmp:
            good, bad = Path(tmp, 'good.json'), Path(tmp, 'bad.json')
            good.write_text(json.dumps(receipt('suspend')))
            bad.write_text('{"schema": 1}')
            for path, code in ((good, 0), (bad, 1)):
                run = subprocess.run([sys.executable, '-I', str(SCRIPT), str(path)], capture_output=True, text=True, check=False)
                self.assertEqual(run.returncode, code, run.stdout + run.stderr)
                self.assertEqual(json.loads(run.stdout)['ok'], code == 0)


if __name__ == '__main__':
    unittest.main()
