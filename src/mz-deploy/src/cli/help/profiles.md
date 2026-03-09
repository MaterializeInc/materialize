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

## Per-profile secret configuration

In `project.toml`, you can configure secret resolution settings per profile
under `[profiles.<name>]`:

```toml
profile = "default"

[profiles.production]
aws_profile = "prod-account"

[profiles.staging]
aws_profile = "staging-account"
```

The `aws_profile` field sets the AWS profile used when resolving secrets
from AWS Secrets Manager via the `aws_secret()` provider.

## Examples

    mz-deploy profiles                    # List profiles, mark default active
    mz-deploy profiles --profile staging  # List profiles, mark staging active

## Related Commands

- `mz-deploy debug` — Test connectivity with the active profile.
- `mz-deploy new` — Scaffold a new project with a `project.toml`.
