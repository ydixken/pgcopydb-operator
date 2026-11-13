# Owned kind qualification

Use this harness for one owned Podman-backed kind cluster.
The legacy bootstrap selects PostgreSQL 17 to 17 with one Ginkgo process.
A native run selects one named spec on the pair 16:16, 18:18 or 16:18 with the native runner, and every other engine and pair is refused.

## Source and image inputs

Use a clean feature checkout that matches the manager publication receipt and image lock.
Use the engine source identified by the native publication receipt.
The static catalog in `test/e2e/kind/images.json` contains approved public dependencies.
The external schema 2 image lock binds the exact source commits, Git trees, payload inventories, selected image manifests, configs, and public receipt hashes.
Keep the lock and receipts outside Git in private owned directories.
The lock must be a mode-0600 regular file without a symlink.
Each receipt must pass the guard's exact schema and provenance checks.

We pull each public image by its selected Linux manifest digest.
PostgreSQL references retain the major tag with the digest, so CNPG can select the major.
The registry index digest, selected manifest digest, and image config digest have separate roles.
A lock records a null index for a single manifest.
Per-node evidence binds the actual CRI image representations before workloads start.
The global CRI connection timeout uses the explicit duration `10s`.
Failed pull diagnostics retain only the constant operation, safe error class, and return code, and the guard discards raw child output.
The suite independently checks the SHA-256 of the exact `RUN/image-lock.json` bytes against `image_lock_sha256`.

> [!important]
> A public source repository does not prove that its package is public.
> Require anonymous manifest, config, and blob checks plus a full pull on every owned node.
> Do not add registry credentials, a local registry, image imports, or image aliases to bypass a refusal.

## Regenerate the image lock

We rebuild the lock from public data, so a lost lock directory is not lost evidence.
`task e2e:kind:lock` keeps the upstream digests of an earlier schema 2 lock, given as `PINS`, and reads their index, manifest, and config bytes anonymously from the registries.
It checks each layer blob with an anonymous `HEAD` request.
It takes the manager and native-runner entries from the publication receipts of the hosted runs `MANAGER_RUN` and `NATIVE_RUN`, which `gh run download` fetches again.
`LEGACY_ANCHORS` names the three original legacy evidence files, which the generator matches by their catalog SHA-256 values.
The generator writes the lock, every receipt, and `lock-manifest.json` with file hashes into a fresh private directory.
Without `OUTPUT_DIR`, that directory is `/var/tmp/rpgmigrate-m7-lock-<uuid>`.

```sh
task e2e:kind:lock PINS="$OLD_LOCK" MANAGER_RUN="$MANAGER_RUN" NATIVE_RUN="$NATIVE_RUN" \
  LEGACY_ANCHORS="$EVIDENCE $QUALIFICATION $PROBE"
```

Run it without `KUBECONFIG`, proxy, or registry overrides, as for the other kind commands.
The output directory is the `IMAGE_LOCK` parent and the `RECEIPT_DIR` for `e2e:kind:prepare`.

## Development publication

The approved feature-branch workflow calls `task image:dev:publish` for the exact checkout and architecture.
It publishes only `ghcr.io/ydixken/pgcopydb-operator:dev-<full-commit>-<architecture>`.
The native runner has its own repository and publication workflow.
An existing development tag must never be overwritten.
Local publication calls refuse outside the admitted workflow context.
The separate native first-package bootstrap exception does not authorize a local manager publisher.
A pending public-visibility receipt does not qualify an image lock.
Release tags, `latest`, main pushes, and package changes outside these two approved development packages remain outside this approval.

## Run the guarded lifecycle

The host requires the qualified Podman VM transport and the pinned tool versions in the guard, including the Podman 6.2.0 client.
The guard checks VM capacity, cgroups, source identity, and private paths before effects.
We read each node's cgroup facts in one `podman machine ssh` call per check, because the qualification stages repeat the full admission hundreds of times and every VM session costs a round trip.
The three nodes' observations, pulls and image verifications run concurrently, and the first failure or stage alarm kills the sibling commands.
The cluster config sets the kubelet `podPidsLimit` to 4096.
We bound PIDs on the pod slice, because systemd gives each container scope its own `TasksMax`, and the native qualification refuses a pod slice whose `pids.max` is not 4096.
Set `IMAGE_LOCK` and `RECEIPT_DIR` to the verified external lock and receipt directory from the admitted publication handoff.
Use the same `RUN_DIR` in all subsequent commands.

> [!warning]
> The guard refuses a `RUN_DIR` under `/tmp`, because a reboot wipes it and the evidence with it.
> Keep runs, locks, and receipts under `/var/tmp` or another persistent private directory.

1. Bind a new run to the explicit lock and receipts.
   Without `RUN_DIR`, the guard creates `/var/tmp/rpgmigrate-m7-run-<uuid>` and prints it as `run_dir`.

   ```sh
   task e2e:kind:prepare IMAGE_LOCK="$IMAGE_LOCK" RECEIPT_DIR="$RECEIPT_DIR"
   ```

2. Set `RUN_DIR` to the printed `run_dir`, then check the prepared inputs.

   ```sh
   task e2e:kind:check RUN_DIR="$RUN_DIR"
   ```

3. Create the owned cluster with a finite lease.

   ```sh
   task e2e:kind:up RUN_DIR="$RUN_DIR" LEASE_SECONDS=7200
   ```

4. Pull and check the public workload images on each owned node.

   ```sh
   task e2e:kind:pull RUN_DIR="$RUN_DIR"
   ```

5. Run the positive legacy bootstrap selection, or one native spec instead.

   ```sh
   task e2e:kind:legacy RUN_DIR="$RUN_DIR"
   ```

   Check out these **examples** of a native run:

   - The suspend fault spec, which also writes its TERM receipt, on pair 16:16:

     ```sh
     task e2e:kind:native RUN_DIR="$RUN_DIR" PAIR=16:16 CASE=fault-suspend
     ```

   - The unattended Automatic cutover on pair 16:18:

     ```sh
     task e2e:kind:native RUN_DIR="$RUN_DIR" PAIR=16:18 CASE=automatic-cutover
     ```

6. Remove only the resources admitted by the guard and retain the evidence.

   ```sh
   task e2e:kind:down RUN_DIR="$RUN_DIR"
   ```

The lease accepts 30 to 7200 seconds.
The Task default is 7200.
No command renews the original clocks or converts an older run.
The lease records a wall and a monotonic expiry.
macOS pauses the monotonic clock while the host sleeps, so a wall-only expiry is refused as a host sleep or clock step.

On macOS, `up`, `pull`, and the legacy and native runs require AC power and hold a `caffeinate -dims` child for the command's lifetime.
The guard refuses when `pmset` does not show that child's system-sleep assertions.
The assertions do not stop a lid-close sleep, so keep the lid open and stay attended for the whole lease.
`CASE` names one entry of `NATIVE_SPECS` in `hack/e2e-kind.py`: `fresh-clone`, `early-manual`, `live-writer`, `automatic-cutover`, `native-refusal`, or `fault-copy`, `fault-stream`, `fault-drain`, `fault-failover`, `fault-suspend` and `fault-delete`.
A native run seeds the native fixture, which leaves out `test/e2e/fixtures/legacy.sql`: the objects the native engine refuses before any database change.
`native-refusal`, which the guard admits on pair 16:16 only, applies that file to the source and expects the Migration to fail with reason `NativeBadConfig`, with neither database changed.
Its budget for the refusal is 15 minutes: the 10 minutes a worker pod may wait for a node, plus 5 for the preflight.
It then deletes the Migration and expects the finalizer to clear within five minutes after an owned inventory, with no cleanup Job and no `rpgm_` slot, publication or `_rpgmigrate_` schema left on either side.
It restores the native fixture afterwards, so other native specs on the same source see the shape they expect.
A native follow spec proves its cleanup positively.
The operator deletes the `rpgmigrate-cleanup-<operation>` Job before the Migration completes, so the suite watches the namespace's Jobs from the first native Migration on and requires one owned by the Migration to have succeeded.
It also requires the status to record the finalized worker, no `rpgm_s_<run>_` slot, `rpgm_p_<run>_` publication or source `_rpgmigrate_<run>` schema, and the target `_rpgmigrate_<run>` schema that cleanup retains.
The shared slot and origin counts follow the engine, and each refuses to count when its filter would not match a name that engine creates.
A native fresh clone expects the seeded large objects to stay on the source, because native runs with large objects off.
The guard binds the engine, pair and full spec text into `suite-input.json`, and `TestE2E` focuses on exactly that spec, so a native run never executes a second one.
Its suite runs with the native runner and the PostgreSQL images of its pair, all pulled and verified by digest on every node during `e2e:kind:pull`.
Its receipt names that spec and carries none of the legacy bootstrap claims; the spec asserts its own outcome.
The sealed default class gives each volume a root-owned 0777 directory, and the native engine and evidence programs refuse that as a work volume root.
So before each native Migration the suite makes a `work` directory with mode 0700 as uid 65532 on a default-class seed claim, and offers a hostPath volume at that directory through its own `WaitForFirstConsumer` class.
We do not pre-bind that volume with a `claimRef`, so the claim binds when its worker schedules.
The operator accepts the binder filling an empty `volumeName` while it confirms a new worker Job, and refuses any other change to the claim.
Kubelet applies no `fsGroup` change to a hostPath volume, so a native kind run does not exercise a CSI work volume with `fsGroup`.
The internal suite Task requires the exact guard-owned run, context, engine, and that engine's positive label.
Do not invoke it as a context bypass.
It writes the `go test -json` stream to `RUN_DIR/go-test.json`, so a failed Go gate keeps its failing assertion.
The ledger records only whether that file exists.
The stream can contain fixture output, so keep it in the private run directory and do not publish it.
A run refuses to start over an existing `go-test.json` or `suite-receipt.json`.
A passing run records the sha256 of the receipt it validated in the ledger.
After teardown, `down` writes `final-receipt.json` only when `suite-receipt.json` still has exactly that sha256, and adds the teardown proof.
It does not validate the receipt again, because those checks need the nodes it has just removed.
After a failed run, `down` tears down and writes no `final-receipt.json`.
On pair 16:16 the native suspend and delete fault specs also write `native-term-16-16-<case>.json` there, and fail unless `hack/native-term-receipt.py` accepts it.

> [!caution]
> Preserve historical failed receipts and old run identities.
> Use a fresh source freeze and run after an input change.
> Do not use generic kind deletion, pruning, ownership repair, or cleanup of another run's resources.

## Qualify the native runner profile and delivery

`hack/native-runtime-qualification.py` runs test-only native observations against a guarded run that has reached `pulled`.
It never activates the native engine and grants no production admission.
Run it from a second checkout, and pass the clean checkout that owns the run as `--legacy-source-root`, because the guard refuses any source that differs from the image lock.
Each phase writes its private inputs and receipts into a separate `QUAL_DIR` outside Git.

1. Bind the run's image lock and the native runner, PostgreSQL 16 and PostgreSQL 18 images to a fresh qualification.

   ```sh
   python3 -B hack/native-runtime-qualification.py --run-dir "$RUN_DIR" \
     --legacy-source-root "$GUARD_ROOT" --qual-dir "$QUAL_DIR" --phase prepare
   ```

2. Run each phase with the run's original lease clocks as the parent deadline.

   ```sh
   for phase in metadata password_tls_probe delivery controller; do
     python3 -B hack/native-runtime-qualification.py --run-dir "$RUN_DIR" \
       --legacy-source-root "$GUARD_ROOT" --qual-dir "$QUAL_DIR" --phase "$phase" \
       --parent-deadline "$LEASE_EXPIRES_AT" --parent-monotonic-deadline "$LEASE_MONOTONIC_EXPIRES_AT"
   done
   ```

The first phase after `prepare` pulls the component images on every node and needs 3300 seconds left on the lease.
Later phases reuse that complete pull receipt and need 2400 seconds.
`metadata` reads the active CRI profile, resolver, HOME and provider facts on all three nodes.
`password_tls_probe` connects to disposable PostgreSQL 16 and 18 servers with SCRAM and verify-full TLS.
`delivery` drives the canonical checked-exec relay and terminal Job proof through fault cases, and remounts a PVC read-only.
`controller` installs its own Migration CRD and runs the real reconciler with the production pod-exec transport against one native PostgreSQL 16 to 18 Migration.
It passes only when the reconciler creates exactly the version and preflight Jobs of one operation, consumes a passing preflight and disposes both helpers.
Its client refuses any other Create, so the phase never claims a work PVC or starts a worker.

> [!important]
> A failed phase keeps its private artifacts and refuses a rerun into the same `QUAL_DIR`.
> Start a new `QUAL_DIR` instead of deleting or overwriting evidence.

## Hosted checks and remaining runtime gate

The `e2e-kind.yml` workflow runs nonempty cluster-free Python and Go contract checks on a hosted Linux runner.
Missing test sources, failed tests, skipped tests, and empty inventories fail that gate.
It has `contents: read` and does not publish images or run a cluster.
The runtime Guard uses `podman machine ssh` for VM operations.
A bounded Linux-native-host transport follow-up must pass before we activate hosted kind runtime.
That runtime path must consume verified public receipts and an external lock from the same clean sources, without rebuilding an image inside the runtime cell.
Offline checks and native image publication do not qualify the legacy migration or the native matrix.
