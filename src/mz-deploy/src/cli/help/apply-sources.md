# apply sources — Create missing sources

Reads source definitions from the project and creates any that don't
exist in the database. Existing sources are skipped. Associated indexes,
grants, and comments are applied for newly created sources.

## Usage

    mz-deploy apply sources

## Behavior

1. Compiles and validates the project (same as `compile`).
2. Collects all `CREATE SOURCE` objects from the plan.
3. Queries the database to determine which sources already exist.
4. Creates missing schemas if needed.
5. For each source that does not exist:
   - Executes the `CREATE SOURCE` statement.
   - Applies associated `CREATE INDEX`, `GRANT`, and `COMMENT`
     statements.
6. Reconciles grants on sources that already exist.

The command is **idempotent** — running it multiple times produces the
same result. Sources that already exist are never modified or recreated.

## Dependencies

`apply sources` creates only sources. Dependency objects such as secrets
and connections must already exist in the database. Create them first
with `apply secrets` and `apply connections`, or use bare `apply` which
handles the full dependency chain automatically.

## Examples

    mz-deploy apply sources           # Create missing sources
    mz-deploy apply sources -v        # Verbose: show executed SQL
    mz-deploy apply sources --dry-run # Print SQL without executing
    mz-deploy apply sources --dry-run --output json  # Machine-readable SQL list

## Error Recovery

- **Missing dependency** — If a source references a connection or secret
  that does not exist, run `apply connections` or `apply secrets` first.
- **Permission denied** — Ensure your profile's role has `CREATE`
  privileges on the target schema.
- **Connection fails** — Check your profile configuration and network
  access to the Materialize region.

## Related Commands

- `mz-deploy apply` — Apply all object types in dependency order.
- `mz-deploy delete` — Drop an object and remove its project file.
