# apply clusters — Converge cluster definitions to match project files

Reads cluster definitions from the `clusters/` directory and converges
the live Materialize state to match. Creates missing clusters, alters
ones whose configuration has drifted, and applies grants and comments
idempotently.

## Usage

    mz-deploy apply clusters

## Behavior

1. Loads all `.sql` files from the `clusters/` directory.
2. For each cluster definition:
   - If the cluster does not exist, creates it.
   - If the cluster exists but size or replication factor has drifted,
     alters it to match.
   - Applies associated `GRANT` statements.
   - Applies associated `COMMENT` statements.
3. Reports status per cluster:
   - `+` created
   - `~` altered (drift detected)
   - `=` up-to-date (no changes needed)

The command is **idempotent** — running it multiple times produces the
same result. Grants and comments are safe to re-apply.

## Examples

    mz-deploy apply clusters      # Converge all cluster definitions
    mz-deploy apply clusters -v   # Verbose: show executed SQL

## Error Recovery

- **Cluster creation fails** — Check that the requested size is valid for
  your Materialize region. Already-created clusters from this run remain.
- **Permission denied** — Ensure your profile's role has `CREATE CLUSTER`
  privileges.

## Related Commands

- `mz-deploy apply` — Apply all object types in dependency order.
- `mz-deploy delete cluster <NAME>` — Drop a cluster and remove its project file.