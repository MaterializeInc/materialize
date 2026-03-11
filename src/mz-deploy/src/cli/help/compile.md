# compile — Validate and type-check SQL without connecting to a remote database

Parses every SQL file in the project, resolves dependencies, builds a
deployment plan, and optionally type-checks against a local Materialize
Docker container. Nothing is executed remotely — this is a pure validation
step suitable for local development and CI pipelines.

## Usage

    mz-deploy compile [FLAGS]

## Behavior

1. Loads all `.sql` files from the `models/` directory tree. If a profile
   is active, per-profile file overrides (`name__<profile>.sql`) are resolved
   first — see `mz-deploy help profiles` for details.
2. Resolves inter-object dependencies and performs a topological sort
   (circular dependencies are rejected).
3. Reports a summary of objects, schemas, and dependencies found.
4. If type checking is enabled (the default), starts a Materialize Docker
   container, loads external types from `types.lock`, and validates every
   statement against it. Skips automatically when Docker is unavailable.

With `-v`, also prints the full dependency graph, deployment order, and
generated SQL plan. A passing `compile` guarantees that `stage` and
`apply` will not fail at the SQL-parsing stage.

## Flags

- `--skip-typecheck` — Disable Docker-based type checking. Faster, but only
  validates syntax and dependency order, not column types or function
  signatures.

## Examples

    mz-deploy compile                  # Full validation with type checking
    mz-deploy compile --skip-typecheck # Syntax + dependency check only
    mz-deploy compile -v               # Verbose: show dependency graph,
                                       #   deployment order, and full SQL plan

## Error Recovery

- **Circular dependency detected** — Restructure your SQL so no two objects
  depend on each other. `compile -v` prints the dependency graph to help
  locate the cycle.
- **Type-check failure** — The reported error mirrors what Materialize would
  return. Fix the SQL, or if the error involves an external dependency, run
  `mz-deploy lock` to refresh `types.lock`.
- **Docker unavailable** — Type checking is skipped with a warning. Install
  Docker or pass `--skip-typecheck` to silence the warning.

## Related Commands

- `mz-deploy stage` — Deploys the compiled plan to a staging environment.
- `mz-deploy test` — Runs unit tests, which also compile the project first.
- `mz-deploy lock` — Refresh `types.lock` for type checking.
