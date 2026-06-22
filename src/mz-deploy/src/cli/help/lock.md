# lock — Resolve external dependencies and generate types.lock

Fetches schema information for explicitly declared external dependencies
and writes a `types.lock` file. This file enables offline type checking
during `compile` and `test`.

External dependencies must be declared in `project.toml` as fully-qualified
object names (e.g., `dependencies = ["db.schema.table"]`). Source tables
(defined via `CREATE TABLE FROM SOURCE`) are auto-discovered and do not
need to be declared.

## Usage

    mz-deploy lock

## Behavior

1. Parses `project.toml` to identify declared dependencies.
2. Auto-discovers source tables defined in the project.
3. Connects to the database.
4. Fetches schemas from the database for all declared and discovered objects.
5. Writes `types.lock` in the project root.

All declared dependencies must exist in the target database. If a declared
dependency is not found, the command fails with a hard error.

If there are no dependencies, the command succeeds with an empty lock file.
The lock file is also automatically regenerated after `apply tables` runs.

## Examples

    mz-deploy lock                   # Refresh types.lock
    mz-deploy lock --profile prod    # From production

## Error Recovery

- **Connection failed** — The command needs a live database to fetch external
  schemas. Verify your profile with `mz-deploy debug`.
- **Declared dependency not found** — A dependency listed in `project.toml`
  does not exist in the database. Check the spelling and ensure the object
  has been created first, then re-run.

## Exit Codes

- **0** — Lock file written successfully, or no dependencies found.
- **1** — Project loading error, connection error, or missing declared dependency.

## Related Commands

- `mz-deploy compile` — Uses `types.lock` for type checking.
- `mz-deploy test` — Uses `types.lock` for test validation.
- `mz-deploy apply tables` — Automatically runs lock after creating tables.
