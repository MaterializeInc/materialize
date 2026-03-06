# apply secrets — Create missing secrets and update existing ones

Reads secret definitions from the project, creates any that don't exist,
and updates all secret values to match the project files. Secret values
that use client-side providers (e.g. `env_var('MY_VAR')`) are resolved
at execution time.

## Usage

    mz-deploy apply secrets

## Behavior

1. Compiles and validates the project (same as `compile`).
2. Collects all `CREATE SECRET` objects from the plan.
3. Creates missing schemas if needed.
4. For each secret:
   - Resolves client-side provider functions (e.g. `env_var`).
   - Executes `CREATE SECRET IF NOT EXISTS` (idempotent create).
   - Executes `ALTER SECRET` to update the value to match the file.
   - Applies any associated `GRANT` and `COMMENT` statements.

The command is **idempotent** — running it multiple times produces the
same result, always converging to the values defined in the project.

## Secret Resolution

Secret values can reference client-side providers instead of inline
strings. Providers are resolved at execution time so that `compile`
works without access to secret values.

    CREATE SECRET my_secret AS env_var('MY_SECRET_VAR');

Supported providers:

- `env_var('NAME')` — Reads from the environment variable `NAME`.

Other expressions are passed through to Materialize unchanged.

## Examples

    mz-deploy apply secrets      # Create/update all secrets
    mz-deploy apply secrets -v   # Verbose: show executed SQL

## Error Recovery

- **Environment variable not set** — Set the required variable and re-run.
  The error message includes the variable name.
- **Non-literal argument** — Provider arguments must be string literals
  (e.g. `env_var('MY_VAR')`), not column references or expressions.
- **Connection fails** — Check your profile configuration and network
  access to the Materialize region.

## Related Commands

- `mz-deploy create-tables` — Create tables, sources, and secrets that
  don't already exist (will not update existing secret values).
- `mz-deploy apply clusters` — Converge cluster definitions similarly.
- `mz-deploy apply roles` — Converge role definitions similarly.
