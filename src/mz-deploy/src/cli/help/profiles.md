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
- **username** — Database user.
- **password** — Database password (optional, supports variable substitution).
- **options** — Optional map of session variables applied to every connection
  (see *Per-profile connection options* below).
- **sslmode** — Optional TLS mode (`disable`, `prefer`, `require`, `verify-ca`,
  or `verify-full`). Defaults to `prefer` for loopback hosts and `require`
  otherwise. See *TLS configuration* below.
- **sslrootcert** — Optional path to a CA bundle PEM file, used only when
  `sslmode` is `verify-ca` or `verify-full`. See *TLS configuration* below.

## Built-in emulator profile

A profile named `emulator` is always available, even with no `profiles.toml`.
It connects to a local Materialize emulator on `localhost:6875` as user
`materialize`, so you can deploy against the emulator with zero setup:

    mz-deploy stage --profile emulator

Defining an `emulator` profile of your own in `profiles.toml` overrides the
built-in.

## Where profiles are stored

Profiles are defined in a `profiles.toml` file. The directory containing
`profiles.toml` is resolved in this order:

1. **`--profiles-dir` CLI flag** — Highest priority.
2. **`MZ_DEPLOY_PROFILES_DIR` environment variable** — Checked if the flag is not set.
3. **`~/.mz`** — Default fallback.

For example:

    # Use an explicit directory
    mz-deploy profiles --profiles-dir /path/to/config

    # Or set via environment variable
    export MZ_DEPLOY_PROFILES_DIR=/path/to/config
    mz-deploy profiles

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
2. **`MZ_DEPLOY_PROFILE` env variable** — Useful for CI environments.
3. **`.mzprofile` file in the project root** — Per-checkout default written
   by `mz-deploy profile set <name>`. Gitignored so each teammate can pick
   their own without touching shared config.

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

## Per-profile connection options

Each profile may define an optional `[options]` table whose key–value pairs
are applied as session variables on every connection. This is the cleanest
way to pin a default cluster or search path for every command that uses a
given profile.

```toml
[staging]
host = "staging.example.com"
username = "deploy_bot"
password = "${STAGING_PASSWORD}"

[staging.options]
cluster = "staging_cluster"
search_path = "public,reporting"
```

Rules:

- **Keys must be valid identifiers** — start with a letter or underscore, then
  letters, digits, or underscores. Invalid keys produce a config-load error.
- **Values are verbatim** — no `${VAR}` expansion. Use the `password` field
  for secrets.
- **Applies to every connection** — every mz-deploy command using this
  profile starts with these session variables set.
- **`cluster` is reserved** — mz-deploy pins every connection to its own
  internal cluster. Any `cluster` value you set here is silently overridden.

## TLS configuration

Two optional fields control TLS: `sslmode` and `sslrootcert`.

`sslmode` follows the PostgreSQL vocabulary (minus `allow`):

| Value | Encrypt | Verify chain | Verify hostname |
|-------|---------|--------------|-----------------|
| `disable` | no | n/a | n/a |
| `prefer` | if offered | no | no |
| `require` | yes | no | no |
| `verify-ca` | yes | yes | no |
| `verify-full` | yes | yes | yes |

When unset, the default is `prefer` for loopback hosts (`localhost`,
`127.0.0.1`, `::1`) and `require` for everything else.

`sslrootcert` is an optional absolute path to a CA bundle in PEM format,
used only by `verify-ca` and `verify-full`. When unset, mz-deploy walks a
short list of platform CA paths (macOS system, Homebrew, Debian/Ubuntu,
RHEL, OpenSUSE) and falls back to OpenSSL's compiled-in defaults.

### Recommended for Materialize Cloud

```toml
[prod]
host = "foo.materialize.cloud"
username = "seth@materialize.com"
password = "${MZ_PROFILE_PROD_PASSWORD}"
sslmode = "verify-full"
```

### Self-hosted with private CA

```toml
[staging]
host = "mz.internal.example.com"
username = "deploy_bot"
password = "${MZ_PROFILE_STAGING_PASSWORD}"
sslmode = "verify-full"
sslrootcert = "/etc/ssl/internal-ca.pem"
```

### Private-network plaintext Materialize

Set `sslmode = "disable"` explicitly. With no `sslmode`, the default
`require` will fail with an error pointing you at this setting.

```toml
[internal]
host = "10.0.1.42"
username = "deploy_bot"
sslmode = "disable"
```

## Per-profile suffix

In `project.toml`, you can configure a per-profile suffix that renames
both databases and clusters at compile time:

```toml
[staging]
profile_suffix = "_staging"
```

- **`profile_suffix`** — Appended to every database name and every cluster
  name. `materialize` becomes `materialize_staging`, and `analytics` becomes
  `analytics_staging`. Also rewrites `IN CLUSTER` references in views,
  sources, sinks, and indexes.

The suffix includes the delimiter — write `"_staging"`, not `"staging"`.

When combined with staging deployments, the profile suffix is applied first,
then the staging suffix stacks on top:
- `profile_suffix = "_staging"` + staging suffix `_a` →
  `foo` → `foo_staging` → `foo_staging_a`

## Per-profile SQL variables

Variables let you parameterize SQL files with values that differ per profile,
using psql-compatible syntax. Define variables in `project.toml`:

```toml
[staging.variables]
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

`compile`, `test`, and `explain` work without an active profile when
your SQL doesn't reference any `:variables` and you haven't configured
`profile_suffix`. As soon as a variable is referenced (or a profile is
explicitly selected), the active profile is required.

## Per-profile secret configuration

In `project.toml`, you can configure secret resolution settings under a
per-profile `[<name>.security]` table:

```toml
[production.security]
aws_profile = "prod-account"

[staging.security]
aws_profile = "staging-account"
```

The `aws_profile` field under `[<name>.security]` sets the AWS profile
used when resolving secrets from AWS Secrets Manager via the
`aws_secret()` provider.

## Per-profile SQL file overrides

SQL files can be specialized per profile using the naming convention
`name#<profile>.sql` (note the **hash**). When a profile is active, its
override replaces the default file of the same object name.

### Naming convention

    name.sql                → default (used when no override matches)
    name#<profile>.sql      → used when <profile> is the active profile

The delimiter is a hash (`#`). The split happens on the **last**
occurrence, and because `#` cannot appear in a SQL identifier, the object
name may freely contain underscores. For example,
`my_pg_conn#staging.sql` splits into object name `my_pg_conn` and profile
`staging`.

### Resolution rules

- **All variants are validated:** Every profile variant of an object is
  loaded and validated at compile time, not just the active one. Invalid
  SQL in a non-active variant is still a compile error.
- **Profile match wins:** If both `name.sql` and `name#<profile>.sql`
  exist and that profile is active, the profile variant is used. Other
  variants are validated but not included in the compiled output.
- **Type consistency enforced:** All variants of an object must share the
  same primary statement type. For example, if `conn.sql` is a
  `CREATE CONNECTION`, then `conn#staging.sql` must also be a
  `CREATE CONNECTION`. A type mismatch is a compile error.
- **No overrides for views or materialized views:** Views and materialized
  views cannot have profile-specific overrides. Use per-profile SQL
  variables instead (see above).
- **Profile-only files are valid:** A file like `secret#staging.sql` with
  no corresponding `secret.sql` default is loaded when `staging` is active
  and skipped for all other profiles.
- **Duplicates are errors:** Two files resolving to the same object name
  for the same profile (e.g., two `conn.sql` defaults) produce an error.

### Scope

Profile file overrides apply to all SQL object directories:

- `models/` — sources, sinks, tables, connections (not views or materialized
  views — see restriction above)
- `clusters/` — cluster definitions
- `roles/` — role definitions
- `network-policies/` — network policy definitions

### Example

Given a `models/` directory with:

    models/materialize/public/pg_conn.sql
    models/materialize/public/pg_conn#staging.sql

Where `pg_conn.sql` connects to a production Postgres replica:

    CREATE CONNECTION pg_conn TO POSTGRES (
        HOST 'prod-replica.internal',
        DATABASE 'app',
        ...
    );

And `pg_conn#staging.sql` connects to a staging replica:

    CREATE CONNECTION pg_conn TO POSTGRES (
        HOST 'staging-replica.internal',
        DATABASE 'app',
        ...
    );

Then:

- `mz-deploy compile --profile staging` loads `pg_conn#staging.sql`
- `mz-deploy compile --profile production` loads `pg_conn.sql`
- `mz-deploy compile --profile default` loads `pg_conn.sql`

## Examples

    mz-deploy profiles                    # List profiles, mark default active
    mz-deploy profiles --profile staging  # List profiles, mark staging active

## Exit Codes

- **0** — Profiles listed successfully.
- **1** — Configuration error.

## Related Commands

- `mz-deploy debug` — Test connectivity with the active profile.
- `mz-deploy new` — Scaffold a new project with a `project.toml`.
