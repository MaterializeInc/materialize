# apply tables — Create missing tables

Reads table definitions from the project and creates any that don't
exist in the database. Existing tables are skipped. Associated indexes,
grants, and comments are applied for newly created tables.

## Usage

    mz-deploy apply tables

## Behavior

1. Compiles and validates the project (same as `compile`).
2. Collects all `CREATE TABLE` and `CREATE TABLE ... FROM SOURCE`
   objects from the plan.
3. Queries the database to determine which tables already exist.
4. Creates missing schemas if needed.
5. For each table that does not exist:
   - Executes the `CREATE TABLE` statement.
   - Applies associated `CREATE INDEX`, `GRANT`, and `COMMENT`
     statements.
6. Reconciles grants on tables that already exist.

The command is **idempotent** — running it multiple times produces the
same result. Tables that already exist are never modified or recreated.

After creating tables, `apply tables` automatically regenerates
`types.lock` so that `compile` and `test` have up-to-date type
information.

## Dependencies

`apply tables` creates only tables. Dependency objects such as secrets,
connections, and sources must already exist in the database. Create them
first with `apply secrets`, `apply connections`, and `apply sources`, or
use bare `apply` which handles the full dependency chain automatically.

## Examples

    mz-deploy apply tables           # Create missing tables
    mz-deploy apply tables -v        # Verbose: show executed SQL
    mz-deploy apply tables --dry-run # Print SQL without executing
    mz-deploy apply tables --dry-run --output json  # Machine-readable SQL list

## Error Recovery

- **Missing dependency** — If a table references a source, connection,
  or secret that does not exist, run `apply sources`, `apply connections`,
  or `apply secrets` first.
- **Permission denied** — Ensure your profile's role has `CREATE`
  privileges on the target schema.
- **Connection fails** — Check your profile configuration and network
  access to the Materialize region.

## Exit Codes

- **0** — All tables created, or no table files found.
- **1** — Compilation, validation, or connection error.

## Related Commands

- `mz-deploy apply` — Apply all object types in dependency order.
- `mz-deploy delete table <NAME>` — Drop a table and remove its project file.
