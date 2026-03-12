---
name: mz-deploy
description: >
  Guide for working with mz-deploy projects that manage Materialize SQL objects
  via blue-green deployments. Use when editing .sql model files, project.toml,
  profiles.toml, clusters/, roles/, or running mz-deploy commands (compile,
  stage, apply, promote, wait, test).
---

# mz-deploy

## Project structure

- `models/<database>/<schema>/` — SQL model files (one primary object per file)
- `clusters/` — Cluster definitions
- `roles/` — Role definitions
- `network-policies/` — Network policies
- `project.toml` — Project settings (profile, Materialize version)

## Model files — one object per file

Each `.sql` file in `models/<database>/<schema>/` defines exactly **one**
primary object. Allowed primary statements:

- `CREATE VIEW`
- `CREATE MATERIALIZED VIEW`
- `CREATE TABLE`
- `CREATE SOURCE`
- `CREATE TABLE FROM SOURCE`
- `CREATE SINK`
- `CREATE INDEX`

The object name **must match the filename** (e.g., `my_view.sql` contains
`CREATE VIEW my_view`). Names are auto-qualified based on the directory path
(`models/materialize/public/my_view.sql` becomes `materialize.public.my_view`).

### Supporting statements

The following supporting statements are allowed in the same file, but they
**must reference the primary object** defined in that file:

- `CREATE INDEX` — indexes on the primary object
- `COMMENT ON` — comments on the object or its columns
- `GRANT` — permissions on the object
- `EXECUTE UNIT TEST` — inline unit tests

### Schema separation rule

A schema **cannot mix** storage objects (tables, sources, sinks) with
computation objects (views, materialized views). Keep them in separate schemas.

## Mod files

### Schema mod file

`models/<database>/<schema_name>.sql` — a file that is a sibling to the
`<schema_name>/` directory. It can contain:

- `SET api = stable` — marks the schema as a **stable API** (replacement
  schema). Only materialized views are allowed in stable API schemas. By
  default, changed objects are deployed via a schema swap, which replaces the
  entire schema and breaks any downstream consumers outside the project.
  Stable API schemas use the replacement protocol instead
  (`ALTER MATERIALIZED VIEW ... APPLY REPLACEMENT`), which atomically replaces
  the MV's internal computation without dropping and recreating it. The MV's
  identity and name stay the same, so downstream objects — both in-project and
  external — are not disrupted or redeployed.
  See `references/stable-api.md` for the recommended two-schema layout pattern.
- `COMMENT ON SCHEMA`
- `GRANT ... ON SCHEMA`
- `ALTER DEFAULT PRIVILEGES IN SCHEMA`

### Database mod file

`models/<database_name>.sql` — a file that is a sibling to the
`<database_name>/` directory. It can contain:

- `COMMENT ON DATABASE`
- `GRANT ... ON DATABASE`
- `ALTER DEFAULT PRIVILEGES IN DATABASE`

## project.toml

```toml
profile = "default"    # Required — active connection profile name
mz_version = "cloud"   # Optional — "cloud"/omitted = latest, "v0.64.0" = that tag
```

## profiles.toml

Profiles are defined in a `profiles.toml` file. The directory containing
`profiles.toml` is resolved in this order:

1. **`--profiles-dir` CLI flag** — Highest priority.
2. **`MZ_DEPLOY_PROFILES_DIR` environment variable** — Checked if the flag is not set.
3. **`~/.mz`** — Default fallback.

```toml
[default]
host = "localhost"          # Required
port = 6875                 # Optional, defaults to 6875
username = "materialize"    # Optional (alias: user)
password = "${MZ_PASSWORD}" # Optional — literal or ${ENV_VAR}
```

Environment variable override: `MZ_PROFILE_<NAME>_PASSWORD` (e.g.,
`MZ_PROFILE_DEFAULT_PASSWORD`).

Profiles also support per-profile SQL variables, file overrides
(`name__<profile>.sql`), and database/cluster suffixes for development in staging environments.
See `mz-deploy help profiles` for details.

## Clusters and roles

- `clusters/<name>.sql` — `CREATE CLUSTER` + optional `GRANT`, `COMMENT`
- `roles/<name>.sql` — `CREATE ROLE` + optional `ALTER ROLE`, `GRANT ROLE`,
  `COMMENT`

## Deployment lifecycle

To deploy changes: `compile` → `test` → `apply` → `stage` → `wait` → `promote`.

1. `mz-deploy compile` — Parse and validate all SQL files locally.
2. `mz-deploy test` — Compile all SQL files and run unit tests locally.
3. `mz-deploy apply` — Create or modify durable state: tables, sources,
   connections, secrets, roles, network policies, and clusters. These persist
   across deployments and are only changed via `apply`.
4. `mz-deploy stage` — Deploy views, materialized views, indexes, and sinks
   into a new "shadow" deployment that runs alongside the current one.
5. `mz-deploy wait` — Wait for all materialized views and indexes in the
   staged deployment to be fully hydrated.
6. `mz-deploy promote` — Swap the staged deployment into the active slot.

Local compilation and unit tests require a types.lock file. types.lock is
automatically generated whenever `mz-deploy apply tables` is run and can be
regenerated with `mz-deploy lock`.

## Unit tests

SQL files can include inline `EXECUTE UNIT TEST` statements that validate
view logic against mock data. Tests run during `mz-deploy test`. Each test
declares mocks for every dependency and an expected result; mz-deploy
validates schemas before execution.

Run `mz-deploy help test` for the full syntax reference, mock resolution
rules, AT TIME usage, and examples.

## Getting detailed command help

Use `mz-deploy help <command>` for detailed, agent-optimized documentation
on any command. Unlike `--help` (which prints brief CLI usage), `help`
returns full guides with behavior notes, examples, error recovery steps,
and related commands.

    mz-deploy help <command>    # Detailed guide for a single command
    mz-deploy help --all        # All command guides concatenated
