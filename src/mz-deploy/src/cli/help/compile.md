# compile — Validate and type-check SQL without connecting to a remote database

Parses every SQL file in the project, resolves dependencies, builds a
deployment plan, and type-checks every statement locally. Nothing is
executed remotely — this is a pure validation step suitable for local
development and CI pipelines.

## Usage

    mz-deploy compile [FLAGS]

## Behavior

1. Loads all `.sql` files from the `models/` directory tree, including every
   profile variant (`name#<profile>.sql`). All variants are validated, then
   the active profile determines which variant is used — see
   `mz-deploy help profiles` for details.
2. Resolves inter-object dependencies and performs a topological sort
   (circular dependencies are rejected).
3. Reports a summary of objects, schemas, and dependencies found.
4. Type-checks every statement using the project's type information. Loads
   external types from `types.lock` and validates column types, function
   signatures, and dependency schemas. Incremental — re-runs are fast.

Every profile variant is validated regardless of `--profile`, so a syntax
error in `foo#staging.sql` will still fail `compile --profile production`.

With `-v`, also prints the full dependency graph, deployment order, and
generated SQL plan. A passing `compile` guarantees that `stage` and
`apply` will not fail at the SQL-parsing stage.

## Examples

    mz-deploy compile      # Full validation with type checking
    mz-deploy compile -v   # Verbose: show dependency graph,
                           #   deployment order, and full SQL plan

## Error Recovery

- **Circular dependency detected** — Restructure your SQL so no two objects
  depend on each other. `compile -v` prints the dependency graph to help
  locate the cycle.
- **Type-check failure** — The reported error mirrors what Materialize would
  return. Fix the SQL, or if the error involves an external dependency, run
  `mz-deploy lock` to refresh `types.lock`.
- **Stale incremental cache** — Delete the `target/` build directory and
  re-run.

## Exit Codes

- **0** — Project parsed, validated, and type-checked successfully.
- **1** — Parse error, validation error, dependency cycle, or type-check failure.

## Related Commands

- `mz-deploy stage` — Deploys the compiled plan to a staging environment.
- `mz-deploy test` — Runs unit tests, which also compile the project first.
- `mz-deploy lock` — Refresh `types.lock` for type checking.
