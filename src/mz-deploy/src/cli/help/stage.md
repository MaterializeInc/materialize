# stage — Create a staging deployment for testing changes

Deploys all views, materialized views, indexes, and related objects to
staging schemas and clusters with a suffixed name (e.g., `public_abc123`).
Tables and sources are not recreated — they must already exist (see
`apply`). Staging deployments run in isolation alongside
production and can be promoted with `deploy` or cleaned up with `abort`.

## Usage

    mz-deploy stage [FLAGS]

## Behavior

1. Validates the git working tree is clean (unless `--allow-dirty`).
2. Compiles and validates the project (same as `compile`).
3. Diffs the plan against the current production snapshot to determine
   what changed. On the first deployment, everything is new.
4. Validates privileges, cluster isolation, and sink connections.
5. Records deployment metadata (object hashes, deferred sinks, replacement
   materialized views).
6. Creates staging resources:
   - Staging schemas with `_<deploy_id>` suffix (e.g., `public_abc123`).
   - Staging clusters cloned from production cluster configuration.
7. Applies schema setup statements (transformed for staging names).
8. Deploys all objects except tables and sources to staging schemas.
9. On failure, automatically rolls back staging schemas and clusters
   (unless `--no-rollback`).

Sinks are deferred to the `deploy` step because they should not start
producing until the deployment is promoted.

### Stable API Schemas (`SET api = stable`)

By default, `stage` recreates every changed object in a staging schema
and `deploy` swaps the entire schema into production. This means any
downstream consumers — views, materialized views, or sinks that other
teams have built on top of objects in that schema — would break, because
the swap replaces the schema and all its contents atomically.

Adding `SET api = stable` to a schema's mod file marks that schema as
a **stable API boundary**. Objects in the schema become safe for
downstream consumers to depend on. When a materialized view in a stable
schema changes, mz-deploy uses Materialize's replacement protocol
instead of a schema swap:

    ALTER MATERIALIZED VIEW ... APPLY REPLACEMENT ...

This atomically replaces the internal computation of the MV without
dropping and recreating it. The MV's identity and name stay the same,
so anything built on top of it — by your team or others — continues
working without interruption.

Key points:

- Only `stable` is supported (`SET api = stable`).
- Schemas with `SET api = stable` can **only** contain materialized
  views — no tables, views, sinks, or sources.
- A changed replacement MV does **not** propagate dirtiness to its
  downstream dependents, so dependent objects are not redeployed.
- Replacement MVs are recorded during `stage` and applied during
  `deploy` after the main schema swap completes.

Example schema mod file (`models/materialize/ontology.sql`):

    SET api = stable;

With this in place, all MVs under `models/materialize/ontology/` are
deployed as replacements when they change. Other teams can safely build
views or sinks on top of these MVs knowing they will never be dropped
during a deployment.

## Flags

- `--deploy-id <ID>` — Custom deployment ID (default: random 7-char hex).
  Used as suffix for schemas and clusters. Must be alphanumeric, hyphens,
  and underscores only.
- `--allow-dirty` — Deploy with uncommitted git changes.
- `--no-rollback` — On failure, leave staging resources in place for
  debugging instead of cleaning them up automatically.
- `--dry-run` — Print the SQL that would be executed without running it.

## Examples

    mz-deploy stage                           # Random deploy ID
    mz-deploy stage --deploy-id dev           # Custom ID → public_dev
    mz-deploy stage --dry-run                 # Preview SQL
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
- `mz-deploy ready` — Monitor staging cluster hydration.
- `mz-deploy deploy` — Promote staging to production.
- `mz-deploy abort` — Clean up a staging deployment.
- `mz-deploy deployments` — List active staging deployments.
