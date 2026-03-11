# apply connections — Create missing connections and reconcile drifted ones

Reads connection definitions from the project, creates any that don't exist,
and reconciles existing connections whose configuration has drifted from the
project file. Uses `SHOW CREATE CONNECTION` to fetch the live state and
diffs option-by-option, emitting `ALTER CONNECTION ... SET/DROP` only for
options that have actually changed.

## Usage

    mz-deploy apply connections

## Behavior

1. Compiles and validates the project (same as `compile`).
2. Collects all `CREATE CONNECTION` objects from the plan.
3. Creates missing schemas if needed.
4. Resolves client-side secret providers in connection options.
5. For each connection:
   - Fetches `SHOW CREATE CONNECTION` for the live state.
   - If the connection does not exist, creates it.
   - If the connection exists, parses the live SQL, diffs options, and
     emits `ALTER CONNECTION ... SET (option)` or `DROP (option)` for
     each difference.
   - Applies associated `GRANT` statements.
   - Applies associated `COMMENT` statements.
6. Reports status per connection:
   - `+` created
   - `~` altered (drift detected)
   - `=` up-to-date (no changes needed)

The command is **idempotent** — running it multiple times produces the
same result. Unnecessary ALTERs are avoided to prevent reconnection
overhead.

## Secret References

Connection options that reference secrets (e.g. `SASL PASSWORD = SECRET
my_secret`) are compared structurally with the live state. `SHOW CREATE
CONNECTION` returns non-redacted SQL with fully-qualified secret names,
which matches the project's normalized format.

## Examples

    mz-deploy apply connections      # Create/reconcile all connections
    mz-deploy apply connections -v   # Verbose: show executed SQL
    mz-deploy apply connections --dry-run                # Print SQL without executing
    mz-deploy apply connections --dry-run --output json  # Machine-readable SQL list

## Error Recovery

- **Connection creation fails** — Check that referenced secrets and SSH
  tunnels exist. Run `mz-deploy apply secrets` first if needed.
- **ALTER fails** — Some option changes may require dropping and
  recreating the connection. Check the Materialize docs for ALTER
  CONNECTION limitations.
- **Permission denied** — Ensure your profile's role has `CREATE`
  privileges on the target schema.

## Exit Codes

- **0** — All connections applied, or no connection files found.
- **1** — Compilation, validation, or connection error.

## Related Commands

- `mz-deploy apply` — Apply all object types in dependency order.
- `mz-deploy delete connection <NAME>` — Drop a connection and remove its project file.