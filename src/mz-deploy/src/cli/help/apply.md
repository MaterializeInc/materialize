# apply — Apply infrastructure objects to Materialize

Declarative, diff-based, idempotent management of infrastructure objects.
Without a subcommand, applies all types in dependency order:
clusters → roles → secrets → connections → sources → tables.

## Usage

    mz-deploy apply [FLAGS]
    mz-deploy apply <TYPE>

## Behavior

When run without a subcommand (`mz-deploy apply`):

1. Applies cluster definitions from `clusters/` — creates missing,
   alters drifted configuration.
2. Applies role definitions from `roles/` — creates missing, applies
   ALTER, GRANT, COMMENT statements.
3. Applies secrets — creates missing, updates values (skip with
   `--skip-secrets`).
4. Applies connections — creates missing, reconciles drifted options.
5. Applies sources — creates missing sources (idempotent).
6. Applies tables — creates missing tables (idempotent).

Each step is idempotent — running `apply` multiple times converges
to the same state.

When run with a subcommand (`mz-deploy apply clusters`), only that
type is applied.

## Subcommands

- `clusters` — Apply cluster definitions from `clusters/` directory.
- `roles` — Apply role definitions from `roles/` directory.
- `secrets` — Create missing secrets and update existing values.
- `connections` — Create missing connections, reconcile drift.
- `sources` — Create sources that don't exist.
- `tables` — Create tables that don't exist.

## Flags

- `--skip-secrets` — Skip applying secrets (bare `apply` only). Useful
  for users without access to secret values.
- `--dry-run` — Print the SQL that would be executed without running it.

## Examples

    mz-deploy apply                     # All infrastructure
    mz-deploy apply --skip-secrets      # Skip secrets
    mz-deploy apply --dry-run           # Preview SQL
    mz-deploy apply clusters            # Clusters only
    mz-deploy apply secrets             # Secrets only
    mz-deploy apply tables              # Tables only

## Error Recovery

- **Connection fails** — Check your profile configuration with
  `mz-deploy debug`.
- **Secret resolution fails** — Ensure environment variables or AWS
  credentials are available, or use `--skip-secrets`.
- **Table/source creation fails** — Already-created objects remain.
  Fix the failing SQL and re-run; existing objects will be skipped.

## Related Commands

- `mz-deploy deploy` — Promote a staging deployment to production.
- `mz-deploy stage` — Deploy views/MVs to staging.
- `mz-deploy compile` — Validate SQL before applying.
- `mz-deploy apply clusters` — Converge cluster definitions.
- `mz-deploy apply roles` — Converge role definitions.
