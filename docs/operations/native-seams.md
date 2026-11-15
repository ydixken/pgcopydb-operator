# Native seam harness

The native seam harness runs the native engine path of the operator against real PostgreSQL and the qualified runner, without a cluster.
It is a gate before a kind run.
Each M7 run found defects at the same seams.
Go gives an environment, an argv or a request to a program, or a program reads state that the engine wrote.

## What the harness does

`TestNativeSeams` in `internal/controller` starts a source and a target PostgreSQL in Podman and loads the native M7 fixture at scale 0.01.
A fake API store holds the Migration, the claim, the Jobs and the Pods.
An HTTP endpoint answers the podexec transport, so the reconciler uses its own exec and log transport.
Podman runs each Pod from the Job that the operator's builder made.

The harness does these runs:

1. A follow run under a live writer.
   It reconciles to a running worker, waits for the streaming phase, and samples lag and the source head through the worker exec.
   It approves the cutover, saves E, sets endpos and waits for the drain.
   Then it reads each completion helper as its own step.
   These helpers are the clone report, the endpos acknowledgment, the completed evidence, both compares, the owned inventory, the cleanup and the cleaned evidence.
   Last, it reconciles to `Completed`, checks both databases and deletes the Migration.
2. A retry run.
   It kills the worker with SIGKILL while it streams, reads the failed worker log and the retry gate, and reconciles to attempt 2.
   Then it deletes the Migration while attempt 2 streams, so the disposition stops the worker and runs the deletion inventory and cleanup.

Each step gives a verdict and the harness continues after a failed step.
When the controller cannot save E because the source head read fails, the harness saves `pg_current_wal_lsn()` from the source and marks the step with a fallback.
The test fails when one or more steps fail.

## Run the harness

You need Podman, a schema 2 image lock, and the pinned images.
The harness pulls an absent image.

1. Choose the pair and the lock.

    ```sh
    task test:native-seams PAIR=16:16 IMAGE_LOCK=/var/tmp/rpgmigrate-m7-lock-<uuid>/image-lock.json
    ```

2. To keep the verdicts, set `EVIDENCE_DIR`.
   The harness writes `native-seams-<pair>.json` there, with the execs and the failed Pods of each failed step.

    ```sh
    task test:native-seams PAIR=16:18 IMAGE_LOCK="$LOCK" EVIDENCE_DIR="$EVIDENCE" OWNER=my-run
    ```

The task fails when the test does not run to a pass, also when a filter selects no test.
Set `NATIVE_SEAMS_TRACE=1` to log each API write and each podexec request.

> [!warning]
> The harness labels each container, volume and network with `io.rpgmigrate.owner=<OWNER>` and deletes them at the end.
> A killed test cannot clean up.
> Delete the objects with that label yourself, and only those.

## What the stand-in kubelet copies

The stand-in kubelet copies the parts of a Pod that the native programs check:

- The `securityContext` user, group, `fsGroup`, read-only root file system, dropped capabilities and `no-new-privileges`.
- Kubelet `$(VAR)` expansion in the command, the arguments and the env, and the `kubernetes` service variables.
- Secret volumes with the atomic writer layout (`..data` symlinks, group read under `fsGroup`).
- An `emptyDir` with medium `Memory` as tmpfs, and other `emptyDir` volumes as setgid 0777 directories.
- The termination message for `FallbackToLogsOnError`.
- Job status with the 1.36 conditions (`SuccessCriteriaMet`, `FailureTarget`).

The work claim is a private volume owned by uid 65532 with mode 0700, as the kind suite makes it.
So, like a kind run, the harness does not exercise a CSI volume with an `fsGroup` change.

The harness does not copy these parts:

- A tmpfs `/tmp` keeps group root, because Podman cannot set the group of a tmpfs mount.
- A Pod log can put stdout and stderr in a different order than kubelet, because `podman logs` keeps the two streams apart.
- The connections do not use TLS.
- The Job controller does not retry: a Job with `backoffLimit` above 0 still runs one Pod.
