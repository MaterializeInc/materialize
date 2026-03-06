# create-tables — Create tables, sources, secrets, and connections that don't already exist

Identifies tables, sources, secrets, and connections defined in the project,
checks which ones already exist in the target database, and creates only the
missing ones. Each run is tracked as a deployment record marked as
"promoted."

## Usage

    mz-deploy create-tables [FLAGS]

## Behavior

1. Validates the git working tree is clean (unless `--allow-dirty`).
2. Compiles and validates the project (same as `compile`).
3. Identifies all table, source, secret, and connection objects in the plan.
4. Queries the database to determine which already exist — existing objects
   are silently skipped.
5. Creates missing schemas if needed.
6. Applies schema setup statements for affected schemas.
7. Creates objects in dependency order: secrets first, then connections, then
   sources, then tables. Secret values that use client-side providers (e.g.
   `env_var('MY_VAR')`) are resolved at execution time.
8. Records created objects in a deployment snapshot marked as "promoted,"
   so future `stage` runs treat them as existing production objects.
9. Automatically runs `gen-data-contracts` to update `types.lock`.

Tables, sources, secrets, and connections are created separately from views
and materialized views because they represent durable state. Once created,
`stage` and `apply` will not attempt to recreate them.

## Flags

- `--deploy-id <ID>` — Custom deployment ID (default: random 7-char hex).
  Must be alphanumeric, hyphens, and underscores only.
- `--allow-dirty` — Allow deployment with uncommitted git changes.
- `--dry-run` — Print the SQL that would be executed without running it.

## Examples

    mz-deploy create-tables                       # Create missing tables
    mz-deploy create-tables --deploy-id init-001  # Custom deploy ID
    mz-deploy create-tables --dry-run             # Preview SQL only

## Error Recovery

- **Deploy ID already exists** — Choose a different `--deploy-id` or omit
  it to generate a random one.
- **Table creation fails** — Tables already created in this run remain.
  Fix the failing SQL and re-run; existing tables will be skipped.
- **Git dirty** — Commit or stash your changes, or pass `--allow-dirty`.

## Secret Resolution

Secret values can reference client-side providers instead of inline
strings. Providers are resolved at execution time so that `compile` works
without access to secret values.

    CREATE SECRET my_secret AS env_var('MY_SECRET_VAR');
    CREATE SECRET my_secret AS aws_secret('my-secret-name');

Supported providers:

- `env_var('NAME')` — Reads from the environment variable `NAME`.
- `aws_secret('NAME')` — Reads from AWS Secrets Manager. Requires
  `aws_profile` to be set in `project.toml`:

      aws_profile = "my-aws-profile"

Unknown functions are passed through to Materialize unchanged.

## Related Commands

- `mz-deploy apply secrets` — Update existing secrets to match project files.
- `mz-deploy stage` — Deploy views/MVs to staging (tables must exist first).
- `mz-deploy gen-data-contracts` — Automatically run after table creation.
- `mz-deploy deployments` — List active deployments including table deploys.
