# profiles — List available connection profiles

Shows all profiles defined in `profiles.toml` and indicates which one is
currently active. Useful for verifying which profiles are configured and
which one commands will use by default.

## Usage

    mz-deploy profiles
    mz-deploy profiles --profile staging

## What is a profile?

A profile is a named set of connection details for a Materialize instance.
Each profile contains:

- **host** — Hostname or IP address of the Materialize instance.
- **port** — Port number (default: 6875).
- **username** — Database user (optional).
- **password** — Database password (optional, supports variable substitution).

## Where profiles are stored

Profiles are defined in a `profiles.toml` file. mz-deploy searches two
locations in order:

1. **Project-local:** `.mz/profiles.toml` in the project directory.
2. **Global:** `~/.mz/profiles.toml` in your home directory.

The first file found is used; they are not merged.

## profiles.toml format

```toml
[default]
host = "localhost"
port = 6875
username = "materialize"

[staging]
host = "staging.example.com"
username = "deploy_bot"
password = "${STAGING_PASSWORD}"

[production]
host = "production.example.com"
username = "deploy_bot"
password = "${PROD_PASSWORD}"
```

## Active profile resolution

The active profile (the one used by commands) is resolved in this order:

1. **`--profile` CLI flag** — Highest priority. Overrides everything.
2. **`profile` field in `project.toml`** — The project default.

For example, if `project.toml` contains `profile = "default"` but you run
`mz-deploy debug --profile staging`, the `staging` profile is used.

## Password resolution

Passwords support two resolution mechanisms:

### Environment variable substitution

Use `${VAR_NAME}` syntax in the password field. The variable is expanded
at connection time:

```toml
[staging]
host = "staging.example.com"
password = "${STAGING_DB_PASSWORD}"
```

### Environment variable override

Set `MZ_PROFILE_<NAME>_PASSWORD` to override any profile's password.
The profile name is uppercased:

    export MZ_PROFILE_STAGING_PASSWORD="my-secret"

This takes precedence over the password in `profiles.toml`, including
`${VAR}` substitution.

## Per-profile database and cluster suffixes

In `project.toml`, you can configure per-profile suffixes that rename
databases and/or clusters at compile time:

```toml
profile = "default"

[profiles.staging]
suffix = "_staging"
cluster_suffix = "_staging"
```

- **`suffix`** — Appended to every database name. `materialize` becomes
  `materialize_staging`.
- **`cluster_suffix`** — Appended to every cluster name. `analytics` becomes
  `analytics_staging`. Also rewrites `IN CLUSTER` references in views,
  sources, sinks, and indexes.

Both suffixes include the delimiter — write `"_staging"`, not `"staging"`.

When combined with staging deployments, the profile suffix is applied first,
then the staging suffix stacks on top:
- `cluster_suffix = "_staging"` + staging suffix `_a` →
  `foo` → `foo_staging` → `foo_staging_a`

## Per-profile SQL variables

Variables let you parameterize SQL files with values that differ per profile,
using psql-compatible syntax. Define variables in `project.toml`:

```toml
[profiles.staging.variables]
cluster = "staging_cluster"
pg_host = "staging-replica.internal"
```

Then reference them in any SQL file:

```sql
-- :name substitutes the raw value
CREATE MATERIALIZED VIEW mv IN CLUSTER :cluster AS SELECT 1;

-- :'name' substitutes as a single-quoted string (with escaping)
CREATE CONNECTION pg TO POSTGRES (HOST :'pg_host');

-- :"name" substitutes as a double-quoted identifier (with escaping)
SELECT * FROM :"my_table";
```

Variable resolution happens before SQL parsing, so the parser sees the
final resolved SQL. Type casts (`::int`) are not confused with variables.
Variables inside string literals and comments are not resolved.

If a SQL file references a variable that is not defined for the active
profile, compilation fails with an error listing all undefined variables.

## Per-profile secret configuration

In `project.toml`, you can configure secret resolution settings per profile
under `[profiles.<name>]`:

```toml
profile = "default"

[profiles.production.security]
aws_profile = "prod-account"

[profiles.staging.security]
aws_profile = "staging-account"
```

The `aws_profile` field under `[profiles.<name>.security]` sets the AWS
profile used when resolving secrets from AWS Secrets Manager via the
`aws_secret()` provider.

## Per-profile SQL file overrides

SQL files can be specialized per profile using the naming convention
`name__<profile>.sql` (note the **double underscore**). When a profile is
active, its override replaces the default file of the same object name.

### Naming convention

    name.sql                → default (used when no override matches)
    name__<profile>.sql     → used when <profile> is the active profile

The delimiter is a double underscore (`__`). The split happens on the
**last** occurrence, so object names may themselves contain underscores.
For example, `my_pg_conn__staging.sql` splits into object name `my_pg_conn`
and profile `staging`.

### Resolution rules

- **Profile match wins:** If both `name.sql` and `name__<profile>.sql`
  exist and that profile is active, the profile variant is loaded.
- **Other profiles are skipped:** Files targeting a different profile
  (e.g., `name__prod.sql` when `--profile staging`) are ignored entirely.
- **Profile-only files are valid:** A file like `secret__staging.sql` with
  no corresponding `secret.sql` default is loaded when `staging` is active
  and skipped for all other profiles.
- **Duplicates are errors:** Two files resolving to the same object name
  for the same profile (e.g., two `conn.sql` defaults) produce an error.

### Scope

Profile file overrides apply to all SQL object directories:

- `models/` — views, materialized views, sources, sinks, tables
- `clusters/` — cluster definitions
- `roles/` — role definitions
- `network_policies/` — network policy definitions

### Example

Given a `models/` directory with:

    models/materialize/public/pg_conn.sql
    models/materialize/public/pg_conn__staging.sql

Where `pg_conn.sql` connects to a production Postgres replica:

    CREATE CONNECTION pg_conn TO POSTGRES (
        HOST 'prod-replica.internal',
        DATABASE 'app',
        ...
    );

And `pg_conn__staging.sql` connects to a staging replica:

    CREATE CONNECTION pg_conn TO POSTGRES (
        HOST 'staging-replica.internal',
        DATABASE 'app',
        ...
    );

Then:

- `mz-deploy compile --profile staging` loads `pg_conn__staging.sql`
- `mz-deploy compile --profile production` loads `pg_conn.sql`
- `mz-deploy compile --profile default` loads `pg_conn.sql`

## Examples

    mz-deploy profiles                    # List profiles, mark default active
    mz-deploy profiles --profile staging  # List profiles, mark staging active

## Related Commands

- `mz-deploy debug` — Test connectivity with the active profile.
- `mz-deploy new` — Scaffold a new project with a `project.toml`.
