# Owned kind qualification

Use this harness for one owned Podman-backed kind cluster.
The legacy bootstrap selects PostgreSQL 17 to 17 with one Ginkgo process.
Native execution remains closed until the separate image, authentication, TLS, PVC, lifecycle, and migration gates pass.

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

The host requires the qualified Podman VM transport and the pinned tool versions in the guard.
The guard checks VM capacity, cgroups, source identity, and private paths before effects.
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

5. Run the positive legacy bootstrap selection.

   ```sh
   task e2e:kind:legacy RUN_DIR="$RUN_DIR"
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

On macOS, `up`, `pull`, and the legacy run require AC power and hold a `caffeinate -dims` child for the command's lifetime.
The guard refuses when `pmset` does not show that child's system-sleep assertions.
The assertions do not stop a lid-close sleep, so keep the lid open and stay attended for the whole lease.
`e2e:kind:native` requests the native route and refuses while its gates remain closed.
The internal suite Task requires the exact guard-owned run, context, engine, and nonempty positive selection.
Do not invoke it as a context bypass.
It writes the `go test -json` stream to `RUN_DIR/go-test.json`, so a failed Go gate keeps its failing assertion.
The ledger records only whether that file exists.
The stream can contain fixture output, so keep it in the private run directory and do not publish it.
A run refuses to start over an existing `go-test.json` or `suite-receipt.json`.
On pair 16:16 the native suspend and delete fault specs also write `native-term-16-16-<case>.json` there, and fail unless `hack/native-term-receipt.py` accepts it.

> [!caution]
> Preserve historical failed receipts and old run identities.
> Use a fresh source freeze and run after an input change.
> Do not use generic kind deletion, pruning, ownership repair, or cleanup of another run's resources.

## Hosted checks and remaining runtime gate

The `e2e-kind.yml` workflow runs nonempty cluster-free Python and Go contract checks on a hosted Linux runner.
Missing test sources, failed tests, skipped tests, and empty inventories fail that gate.
It has `contents: read` and does not publish images or run a cluster.
The runtime Guard uses `podman machine ssh` for VM operations.
A bounded Linux-native-host transport follow-up must pass before we activate hosted kind runtime.
That runtime path must consume verified public receipts and an external lock from the same clean sources, without rebuilding an image inside the runtime cell.
Offline checks and native image publication do not qualify the legacy migration or the native matrix.
