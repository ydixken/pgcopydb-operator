#!/usr/bin/env python3
"""Check one native worker TERM receipt captured by a kind RUN.

The receipt format and the reason for each check are in the engine plan
docs/superpowers/plans/2026-11-12-m6-native-term-runtime-receipt.md.
Exit status 0 means every check passed; the verdict is printed as JSON.
"""
import datetime
import hashlib
import json
from pathlib import Path
import re
import sys

SCHEMA = 'rpgmigrate.native-term-receipt/v1'
WORKER_CONTAINER = 'pgcopydb'
# Engine ErrorKind exit codes, read off rpgmigrate src/contracts.rs exit_code() at b463abd.
ENGINE_EXIT_CODES = {1, 2, 3, 4, 5, 6, 7, 9, 12, 122}
# The 90 s retained-stop poll bound plus 30 s for the next reconcile to observe the intent.
TERM_WINDOW_SECONDS = 120
DIGEST = re.compile(r'@(sha256:[0-9a-f]{64})$')


def _time(value):
    if not isinstance(value, str) or not value.endswith('Z'):
        raise ValueError(f'not an RFC 3339 UTC time: {value!r}')
    return datetime.datetime.fromisoformat(value[:-1] + '+00:00')


def _uid(obj):
    return (obj or {}).get('metadata', {}).get('uid')


def _controller(obj, kind, uid):
    refs = [r for r in obj.get('metadata', {}).get('ownerReferences', []) if r.get('controller')]
    return len(refs) == 1 and refs[0].get('kind') == kind and refs[0].get('uid') == uid


def _worker(pod):
    statuses = pod.get('status', {}).get('containerStatuses', [])
    if len(statuses) != 1 or statuses[0].get('name') != WORKER_CONTAINER:
        raise ValueError('worker Pod must have exactly one worker container status')
    return statuses[0]


def _receipts(m):
    s = m.get('status', {})
    return (s.get('nativeJobUID'), s.get('nativePVCUID'), s.get('jobName'), s.get('attempts'), (s.get('replication') or {}).get('endpos'))


def _condition(job, kind):
    return any(c.get('type') == kind and c.get('status') == 'True' for c in job.get('status', {}).get('conditions', []))


def check(receipt):
    failures = []

    def need(ok, name):
        if not ok:
            failures.append(name)

    need(receipt.get('schema') == SCHEMA, 'schema')
    case = receipt.get('case')
    need(case in ('suspend', 'delete'), 'case')
    digest = DIGEST.search(receipt.get('runnerImage', ''))
    need(digest is not None, 'runner-digest')
    requested = _time(receipt.get('requestedAt'))
    before, term, after = receipt['before'], receipt['terminated'], receipt['after']
    m, job, pod, pvc = before['migration'], before['job'], before['pod'], before['pvc']

    need(_receipts(m)[:2] == (_uid(job), _uid(pvc)) and None not in _receipts(m)[:2], 'before-receipts')
    need(_controller(job, 'Migration', _uid(m)) and _controller(pvc, 'Migration', _uid(m)) and _controller(pod, 'Job', _uid(job)), 'before-ownership')
    running = _worker(pod)
    need(pod.get('status', {}).get('phase') == 'Running' and 'running' in running.get('state', {}) and running.get('restartCount') == 0, 'before-running')
    need(digest is None or running.get('imageID', '').endswith(digest.group(1)), 'before-image')
    need(not m.get('spec', {}).get('suspend') and not m['metadata'].get('deletionTimestamp'), 'before-intent')

    stopped = _worker(term['pod'])
    exited = stopped.get('state', {}).get('terminated') or {}
    need(_uid(term['pod']) == _uid(pod) and _uid(term['job']) == _uid(job), 'term-identity')
    need(stopped.get('containerID') == running.get('containerID') and stopped.get('restartCount') == 0, 'term-same-container')
    # Exit 0 is the natural-completion race, 137 and 143 are signal deaths: none proves handled TERM.
    need(exited.get('exitCode') in ENGINE_EXIT_CODES and not exited.get('signal'), 'term-engine-exit')
    finished = _time(exited['finishedAt']) if 'finishedAt' in exited else None
    need(finished is not None and 0 <= (finished - requested).total_seconds() <= TERM_WINDOW_SECONDS, 'term-window')
    intent = term.get('migration') or {}
    need(_uid(intent) == _uid(m) and _receipts(intent) == _receipts(m), 'term-receipts-retained')
    if case == 'suspend':
        need(intent.get('spec', {}).get('suspend') is True, 'term-follows-intent')
    else:
        need(bool(intent.get('metadata', {}).get('deletionTimestamp')) and 'pgcopydb-operator.io/cleanup' in intent['metadata'].get('finalizers', []), 'term-follows-intent')

    if case == 'suspend':
        am, aj, ap, ac = after['migration'], after['job'], after['pod'], after['pvc']
        need(am.get('status', {}).get('phase') == 'Suspended' and _receipts(am) == _receipts(m), 'after-suspended-retained')
        need((_uid(aj), _uid(ap), _uid(ac)) == (_uid(job), _uid(pod), _uid(pvc)), 'after-resources-retained')
        need(not any(o['metadata'].get('deletionTimestamp') for o in (aj, ap, ac)), 'after-not-deleting')
        need(not aj.get('spec', {}).get('suspend') and aj.get('spec', {}).get('ttlSecondsAfterFinished') is None and _condition(aj, 'Failed'), 'after-job-retained-failed')
        need(_worker(ap).get('containerID') == running.get('containerID'), 'after-same-container')
    else:
        need(after.get('migration') is None, 'after-released')

    need(sorted(map(json.dumps, receipt['foreign']['before'])) == sorted(map(json.dumps, receipt['foreign']['after'])) and receipt['foreign']['before'], 'foreign-unchanged')
    return failures


def main(argv):
    if len(argv) != 2:
        print('usage: native-term-receipt.py RECEIPT.json', file=sys.stderr)
        return 2
    raw = Path(argv[1]).read_bytes()
    try:
        failures = check(json.loads(raw))
    except (KeyError, TypeError, ValueError, AttributeError) as error:
        failures = [f'malformed: {error}']
    print(json.dumps({'ok': not failures, 'failures': failures, 'receipt_sha256': hashlib.sha256(raw).hexdigest()}))
    return 0 if not failures else 1


if __name__ == '__main__':
    sys.exit(main(sys.argv))
