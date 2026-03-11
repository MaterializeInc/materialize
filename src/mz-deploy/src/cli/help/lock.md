# lock — Generate types.lock for external dependencies

Queries the database for schema information about external dependencies
(tables and views referenced in your SQL but not managed by this project)
and writes a `types.lock` file. This file enables offline type checking
during `compile` and `test`.

## Usage

    mz-deploy lock

## Behavior

1. Parses the project to identify external dependencies.
2. Connects to the database.
3. Queries column names, types, and nullability for each external object.
4. Writes `types.lock` in the project root.

If there are no external dependencies, the command succeeds with an empty
lock file. The lock file is also automatically regenerated after
`apply tables` runs.

## Examples

    mz-deploy lock                   # Refresh types.lock
    mz-deploy lock --profile prod    # From production

## Error Recovery

- **Connection failed** — The command needs a live database to introspect
  external schemas. Verify your profile with `mz-deploy debug`.
- **External object not found** — The referenced table or view does not
  exist in the database yet. Create it first, then re-run.

## Exit Codes

- **0** — Lock file written successfully, or no external dependencies found.
- **1** — Project loading error or connection error.

## Related Commands

- `mz-deploy compile` — Uses `types.lock` for type checking.
- `mz-deploy test` — Uses `types.lock` for test validation.
- `mz-deploy apply tables` — Automatically runs lock after creating tables.
