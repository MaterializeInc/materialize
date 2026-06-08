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
    CREATE SECRET my_secret AS aws_secret('my-secret-name');
    CREATE SECRET db_pw     AS aws_secret('rds-creds', 'password');
    CREATE SECRET api_key   AS gcp_secret('shared-api-key');
    CREATE SECRET pinned    AS gcp_secret('projects/p/secrets/db-pw/versions/3');

Supported providers:

- `env_var('NAME')` — Reads from the environment variable `NAME`.
- `aws_secret('NAME')` — Reads from AWS Secrets Manager. Requires
  `aws_profile` to be set under `[<name>.security]` in `project.toml`:

      [default.security]
      aws_profile = "my-aws-profile"

- `aws_secret('NAME', 'FIELD')` — Reads from AWS Secrets Manager, parses
  the secret as JSON, and returns the top-level string `FIELD` (e.g. for
  RDS-style credentials).

- `gcp_secret('NAME')` — Reads from Google Cloud Secret Manager. With an
  optional default project configured, bare names resolve to the
  `latest` version under that project:

      [default.security]
      gcp_project = "my-gcp-project"

  Pass a full resource path to override the configured project (or to
  work without one), and optionally pin a version:

      CREATE SECRET s AS gcp_secret('projects/other-proj/secrets/api-key');
      CREATE SECRET s AS gcp_secret('projects/p/secrets/db-pw/versions/3');

  Credentials are resolved via Application Default Credentials
  (`gcloud auth application-default login`, `GOOGLE_APPLICATION_CREDENTIALS`,
  or workload identity on GCE/GKE).

- `gcp_secret('NAME', 'FIELD')` — Reads from Google Cloud Secret Manager,
  parses the secret as JSON, and returns the top-level string `FIELD`.

Other expressions are passed through to Materialize unchanged.

## Examples

    mz-deploy apply secrets      # Create/update all secrets
    mz-deploy apply secrets -v   # Verbose: show executed SQL
    mz-deploy apply secrets --dry-run                # Print SQL without executing
    mz-deploy apply secrets --dry-run --output json  # Machine-readable SQL list

## Error Recovery

- **Environment variable not set** — Set the required variable and re-run.
  The error message includes the variable name.
- **`gcp_secret` "no `gcp_project` configured"** — Either set
  `gcp_project` under `[<profile>.security]` or pass a full
  `projects/.../secrets/...` resource path.
- **Non-literal argument** — Provider arguments must be string literals
  (e.g. `env_var('MY_VAR')`), not column references or expressions.
- **Connection fails** — Check your profile configuration and network
  access to the Materialize region.

## Exit Codes

- **0** — All secrets applied, or no secret files found.
- **1** — Compilation, validation, or connection error.

## Related Commands

- `mz-deploy apply` — Apply all object types in dependency order.
- `mz-deploy delete secret <NAME>` — Drop a secret and remove its project file.
