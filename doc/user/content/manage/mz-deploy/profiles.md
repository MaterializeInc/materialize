---
title: "Profiles"
description: "Configure multiple environments with profiles, variables, and file overrides."
menu:
  main:
    parent: manage-mz-deploy
    weight: 60
    identifier: "mz-deploy-profiles"
    name: "Profiles"
---

Profiles let you target different Materialize environments (staging, production)
from the same project. Each profile defines connection details and can customize
cluster sizes, connection hosts, and secret resolution.

## Multiple profiles

Define profiles in `profiles.toml`. Each section header is a profile name:

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

The active profile is resolved in this order:

1. The `--profile` flag on the command line.
2. The `MZ_DEPLOY_PROFILE` environment variable.
3. The per-checkout default recorded by `mz-deploy profile set`.

Each developer on a team sets their own default without touching shared
configuration:

```bash
mz-deploy profile list         # show every profile and which one is active
mz-deploy profile set staging  # record `staging` as the default for this checkout
mz-deploy profile current      # confirm what will be used and where it came from
```

## Built-in emulator profile

A profile named `emulator` is always available, even before you write a
`profiles.toml`. It connects to a local Materialize emulator on `localhost:6875`
as user `materialize`, so you can deploy against the emulator with no
configuration:

```bash
mz-deploy stage --profile emulator
```

Defining your own `emulator` profile in `profiles.toml` overrides the built-in.

## Password resolution

The `password` field in `profiles.toml` supports `${VAR_NAME}` substitution.
Variables are expanded at connection time, so you never need to store secrets in
the file itself.

You can also override the password for any profile with the environment variable
`MZ_PROFILE_<NAME>_PASSWORD`, where the profile name is uppercased. This takes
precedence over the value in `profiles.toml`.

```bash
export MZ_PROFILE_STAGING_PASSWORD="my-secret"
```

## Profile suffixes

Set `profile_suffix` in `project.toml` to rename databases and clusters when
deploying with a particular profile:

```toml
[profiles.staging]
profile_suffix = "_staging"
```

With this configuration, `materialize` becomes `materialize_staging` and `orders`
becomes `orders_staging`. The suffix also rewrites `IN CLUSTER` references in
views, sources, sinks, and indexes.

Note that the suffix includes the delimiter — write `"_staging"`, not
`"staging"`.

When you combine a profile suffix with the staging deploy suffix, the names
stack: `foo` becomes `foo_staging`, then `foo_staging_a1b2c3d`.

## Per-profile SQL variables

Variables parameterize values that differ across profiles. Define them in
`project.toml`:

```toml
[profiles.staging.variables]
compute_cluster = "staging_compute"

[profiles.production.variables]
compute_cluster = "production_compute"
```

Reference variables in SQL using psql-compatible syntax:

```sql
-- models/materialize/public/order_summary.sql
CREATE MATERIALIZED VIEW order_summary
    IN CLUSTER :"compute_cluster" AS
SELECT ...;
```

Three substitution forms are available:

- `:var` — raw value, inserted as-is.
- `:'var'` — single-quoted string with escaping.
- `:"var"` — double-quoted identifier with escaping.

Variables resolve before SQL parsing. If a SQL file references a variable that is
not defined for the active profile, compilation fails with an error.

## Per-profile file overrides

For objects that differ structurally across environments (for example, connections
pointing to different hosts), use file overrides. Name the variant file with a
double-underscore suffix: `name__<profile>.sql`.

```nofmt
models/materialize/public/pg_conn.sql
models/materialize/public/pg_conn__staging.sql
```

```sql
-- models/materialize/public/pg_conn.sql (production)
CREATE CONNECTION pg_conn TO POSTGRES (
    HOST 'prod-replica.internal',
    DATABASE 'app',
    USER SECRET pg_user,
    PASSWORD SECRET pg_password,
    SSL MODE 'require'
);
```

```sql
-- models/materialize/public/pg_conn__staging.sql
CREATE CONNECTION pg_conn TO POSTGRES (
    HOST 'staging-replica.internal',
    DATABASE 'app',
    USER SECRET pg_user,
    PASSWORD SECRET pg_password,
    SSL MODE 'require'
);
```

Resolution rules:

- All variants are validated at compile time, not just the active one.
- When the active profile matches a variant, that variant wins.
- All variants must share the same primary statement type.
- Views and materialized views cannot have overrides — use variables instead.

File overrides apply to: `models/` (sources, sinks, tables, connections),
`clusters/`, `roles/`, and `network-policies/`.

## Per-profile secret configuration

You can configure which AWS profile is used when resolving `aws_secret()`
providers:

```toml
[profiles.production.security]
aws_profile = "prod-account"

[profiles.staging.security]
aws_profile = "staging-account"
```

The `aws_profile` setting controls which AWS profile is used at secret-resolution
time. Different environments can pull secrets from different AWS accounts.
