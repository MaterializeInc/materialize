# stage — Create a staging deployment for testing changes

Compares your project against the current production snapshot and deploys
only the objects that have changed to staging schemas and clusters with a
suffixed name (e.g., `public_abc123`). Unchanged objects are skipped
entirely.

Tables and sources are not recreated — they must already exist (see
`apply`). Staging deployments run in isolation alongside production and
can be promoted with `promote` or cleaned up with `abort`.

## Usage

    mz-deploy stage [FLAGS]

## Behavior

1. Validates the git working tree is clean (unless `--allow-dirty`).
2. Compiles and validates the project (same as `compile`). The connection
   profile's name is used for file resolution, so `--profile staging` will
   load `__staging` file overrides. See `mz-deploy help profiles`.
3. Diffs the plan against the current production snapshot by comparing
   hashes of each object's SQL against the last promoted snapshot. Only
   objects whose definition changed (or whose dependencies changed) are
   included in the staging deployment. Unchanged objects are not
   recreated. On the first deployment, everything is new.
4. Validates privileges, cluster isolation, and sink connections.
5. Records deployment metadata (object hashes, deferred sinks, replacement
   materialized views).
6. Creates staging resources:
   - Staging schemas with `_<deploy_id>` suffix (e.g., `public_abc123`).
   - Staging clusters cloned from production cluster configuration.
7. Applies schema setup statements (transformed for staging names).
8. Deploys changed objects (except tables and sources) to staging schemas.
9. On failure, automatically rolls back staging schemas and clusters
   (unless `--no-rollback`).

Sinks are deferred to the `promote` step because they should not start
producing until the deployment is promoted.

### Stable API Schemas (`SET api = stable`)

By default, when an object changes, `stage` recreates it in a staging
schema and `promote` swaps the entire schema into production. Any
dependents of the changed objects within the same mz-deploy project are
detected as dirty and redeployed automatically, so nothing breaks — but
as projects grow this can cause large cascading redeployments. Consumers
in **other** mz-deploy projects will break, because this project has no
knowledge of them and cannot redeploy them.

`SET api = stable` marks a schema as a **stable API boundary** that
other teams can safely depend on. When a materialized view in a stable
schema changes, mz-deploy uses Materialize's replacement protocol
instead of a schema swap:

    ALTER MATERIALIZED VIEW ... APPLY REPLACEMENT ...

The MV's computation is updated in place and its identity is preserved.
Downstream consumers — whether in the same project or a different one —
do not need to be redeployed and do not need to know about the update.

Key points:

- Only `stable` is supported (`SET api = stable`).
- Schemas with `SET api = stable` can **only** contain materialized
  views — no tables, views, sinks, or sources.
- A changed replacement MV does **not** propagate dirtiness to its
  downstream dependents, so dependent objects are not redeployed.
- Replacement MVs are recorded during `stage` and applied during
  `promote` after the main schema swap completes.

Example schema mod file (`models/materialize/ontology.sql`):

    SET api = stable;

With this in place, all MVs under `models/materialize/ontology/` are
deployed as replacements when they change. Other teams can safely build
views or sinks on top of these MVs knowing they will never be dropped
during a deployment.

## Flags

- `--deploy-id <ID>` — Custom deployment ID. In a git repository,
  defaults to the first 7 characters of the current commit SHA;
  otherwise, a random 7-char hex string is generated.
  Used as suffix for schemas and clusters. Must be alphanumeric, hyphens,
  and underscores only.
- `--allow-dirty` — Deploy with uncommitted git changes.
- `--no-rollback` — On failure, leave staging resources in place for
  debugging instead of cleaning them up automatically.
- `--dry-run` — Preview what would be deployed without executing any
  changes. Combine with `--output json` for machine-readable output
  including staging schemas, clusters, objects, deferred sinks, and
  replacement MVs.

## Concurrent Deployments

Multiple staging deployments can run in parallel. Each deployment gets
its own suffixed schemas and clusters (e.g., `public_abc123`,
`public_def456`), so independent deployments — ones that touch different
schemas and clusters — do not interact at all.

Deployments operate at the schema and cluster level: if one object in a
schema changes, the entire schema is redeployed; if one object on a
cluster changes, the entire cluster is redeployed. Two deployments
"overlap" when they touch any of the same schemas or clusters — even if
they modify different objects within them.

When two deployments overlap, conflict detection at `promote` time
ensures safety: the first deployment to promote wins, and the second is
rejected with a conflict error because production was modified after it
was staged. Re-stage to pick up the latest production state and try
again.

## Examples

    mz-deploy stage                           # Defaults to git SHA prefix
    mz-deploy stage --deploy-id dev           # Custom ID → public_dev
    mz-deploy stage --dry-run                 # Preview SQL
    mz-deploy stage --dry-run --output json  # Machine-readable plan
    mz-deploy stage --no-rollback             # Keep resources on failure

## Error Recovery

- **Git dirty** — Commit or stash changes, or pass `--allow-dirty`.
- **Deploy ID already exists** — Use a different `--deploy-id` or run
  `mz-deploy abort <ID>` to clean up the existing deployment.
- **Staging fails and rolls back** — Fix the SQL and re-stage. Automatic
  rollback removes schemas and clusters created during this attempt.
- **Staging fails with `--no-rollback`** — Inspect the partial deployment,
  then run `mz-deploy abort <ID>` to clean up manually.

## Related Commands

- `mz-deploy compile` — Validate SQL before staging.
- `mz-deploy apply` — Create tables, sources, and other infra before staging.
- `mz-deploy wait` — Monitor staging cluster hydration.
- `mz-deploy promote` — Promote staging to production.
- `mz-deploy abort` — Clean up a staging deployment.
- `mz-deploy list` — List active staging deployments.
