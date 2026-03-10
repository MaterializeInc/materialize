# promote — Promote a staging deployment to production

Atomically swaps a staging deployment into production using `ALTER ... SWAP`.
Supports resumable execution — if interrupted, re-running the same command
picks up where it left off.

## Usage

    mz-deploy promote <DEPLOY_ID> [FLAGS]

## Behavior

1. Validates the deployment exists and has not already been promoted.
2. Runs a readiness check (unless `--no-ready-check`): all staging clusters
   must be hydrated and within the `--allowed-lag` threshold.
3. Detects conflicts — checks whether production schemas were touched by
   another deployment after this staging deployment was created. Use
   `--force` to skip this check.
4. Executes an atomic swap inside a transaction:
   - Swaps user schemas (production ↔ staging).
   - Swaps clusters (production ↔ staging).
   - Swaps apply-state tracking schemas atomically.
5. Post-swap work:
   - Creates deferred sinks (held back during `stage`).
   - Applies replacement materialized views — for schemas marked
     with `SET api = stable`, updates each changed MV in place via
     `ALTER MATERIALIZED VIEW ... APPLY REPLACEMENT` so downstream
     consumers are never disrupted (see `mz-deploy help stage`).
   - Repoints sinks that depended on old production objects.
   - Records the promotion timestamp.
   - Drops the old production resources (now in staging names).
6. Cleans up apply-state tracking tables.

The command is **resumable**: if it crashes after the swap but before
cleanup, re-running `mz-deploy promote <DEPLOY_ID>` detects the post-swap
state and skips directly to step 5.

## Flags

- `--force` — Skip conflict detection. May overwrite changes made to
  production after the staging deployment was created.
- `--no-ready-check` — Skip the readiness/hydration check before promoting.
- `--allowed-lag <SECONDS>` — Maximum wallclock lag (in seconds) for the
  readiness check (default: 300 = 5 minutes).
- `--dry-run` — Preview the deployment plan without executing any changes.
  Connects to the database to discover resources, then prints what would
  be swapped, created, repointed, and dropped. Combine with
  `--output json` for machine-readable output suitable for CI pipelines.

## Examples

    mz-deploy promote abc123                    # Promote staging deployment
    mz-deploy promote abc123 --no-ready-check       # Skip hydration check
    mz-deploy promote abc123 --force            # Ignore conflicts
    mz-deploy promote abc123 --allowed-lag 600  # 10 min lag tolerance
    mz-deploy promote abc123 --dry-run                # Preview plan (text)
    mz-deploy promote abc123 --dry-run --output json  # Machine-readable plan

## CI/CD Usage

Use `--dry-run --output json` to integrate deployment plans into CI
pipelines. The JSON output includes structured arrays for each operation
type: schema swaps, cluster swaps, sinks to create, replacement MVs,
sinks to repoint, and resources to drop.

    # Save deployment plan as artifact
    mz-deploy promote abc123 --dry-run --output json > plan.json

## Error Recovery

- **Staging environment not found** — Verify the deploy ID with
  `mz-deploy list`.
- **Already promoted** — The deployment was already deployed. Check
  `mz-deploy log` for confirmation.
- **Deployment conflict** — Another deployment modified production schemas.
  Review with `mz-deploy log`, then re-run with `--force` if the
  conflict is acceptable.
- **Clusters not ready** — Wait for hydration with `mz-deploy wait <ID>`
  or pass `--no-ready-check` to promote anyway.
- **Interrupted after swap** — Re-run the same `deploy` command. It will
  detect the post-swap state and resume cleanup.
- **Sink creation fails post-swap** — The swap already succeeded. Fix the
  sink definition and re-run `deploy` to retry deferred work.

## Rollback

There is no dedicated rollback command. To revert a promotion, reverse the
changes in your project and promote the result:

    git revert <commit>              # Create a new commit undoing the change
    mz-deploy stage                  # Stage the reverted project
    mz-deploy promote <NEW_DEPLOY_ID> # Promote to production

Because `deploy` uses atomic `ALTER ... SWAP`, the rollback promotion is
itself atomic — production traffic switches back in a single transaction.

## Related Commands

- `mz-deploy stage` — Create the staging deployment to promote.
- `mz-deploy wait` — Monitor hydration before promoting.
- `mz-deploy abort` — Clean up a staging deployment without promoting.
- `mz-deploy apply` — Apply infrastructure objects (clusters, roles, etc.).
