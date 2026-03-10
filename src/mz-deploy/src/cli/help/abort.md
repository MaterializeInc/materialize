# abort — Clean up a staging deployment

Drops all staging schemas, clusters, and deployment tracking records for
the specified deploy ID. This is the equivalent of `git branch -D` for
staging deployments — it permanently removes the staging environment.

## Usage

    mz-deploy abort <DEPLOY_ID>

## Flags

- `--output json` — Print abort summary as JSON to stdout.

## Behavior

1. Validates the deployment exists and has not been promoted.
2. Queries for staging schemas with the `_<deploy_id>` suffix.
3. Queries for staging clusters with the `_<deploy_id>` suffix.
4. Drops all staging schemas with `CASCADE`.
5. Drops all staging clusters with `CASCADE`.
6. Deletes tracking records:
   - Cluster mappings
   - Pending statements (deferred sinks)
   - Replacement materialized view records
   - Apply state schemas
   - Main deployment record

Cleanup is best-effort — if a schema or cluster drop fails, the error is
reported as a warning and cleanup continues. The command can be run
multiple times safely.

## Examples

    mz-deploy abort abc123                   # Clean up staging deployment
    mz-deploy abort abc123 --profile prod    # Use a specific profile
    mz-deploy abort abc123 --output json       # Machine-readable output

## Error Recovery

- **Staging environment not found** — Verify the deploy ID with
  `mz-deploy list`.
- **Already promoted** — Promoted deployments cannot be aborted. The
  resources have already been swapped into production.
- **Partial cleanup** — If some drops fail, re-run `abort` to retry.
  The command is idempotent for already-dropped resources.

## Related Commands

- `mz-deploy stage` — Create a staging deployment.
- `mz-deploy list` — List active staging deployments.
- `mz-deploy deploy` — Promote instead of aborting.
