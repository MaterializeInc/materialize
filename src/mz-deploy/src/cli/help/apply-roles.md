# apply roles — Converge role definitions to match project files

Reads role definitions from the `roles/` directory and converges the
live Materialize state to match. Creates missing roles, applies ALTER
and GRANT statements, and cleans up stale grants and session defaults.

## Usage

    mz-deploy apply roles

## Behavior

1. Loads all `.sql` files from the `roles/` directory.
2. For each role definition:
   - If the role does not exist, creates it.
   - Applies `ALTER ROLE` statements.
   - Applies `GRANT ROLE` statements.
   - Applies `COMMENT` statements.
   - Queries current role members, revokes stale ones not in the
     definition.
   - Queries current session defaults, resets stale parameters not in
     the definition.
3. Reports status per role:
   - `+` created
   - `=` exists (statements applied idempotently)
   - `-` revoked stale member or reset stale default

The command is **idempotent** — running it multiple times produces the
same result.

## Examples

    mz-deploy apply roles      # Converge all role definitions
    mz-deploy apply roles -v   # Verbose: show executed SQL
    mz-deploy apply roles --dry-run                # Print SQL without executing
    mz-deploy apply roles --dry-run --output json  # Machine-readable SQL list

## Error Recovery

- **Role creation fails** — Check that your profile's role has
  `CREATEROLE` or superuser privileges.
- **Revoke fails** — The stale grant may have already been removed.
  Re-running the command will skip it.

## Related Commands

- `mz-deploy apply` — Apply all object types in dependency order.
- `mz-deploy delete role <NAME>` — Drop a role and remove its project file.