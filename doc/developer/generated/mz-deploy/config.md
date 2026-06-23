---
source: src/mz-deploy/src/config.rs
revision: b0390d141f
---

# mz-deploy::config

Configuration loading for profiles and project settings.

`Profile` holds resolved connection details (host, port, credentials, optional `sslrootcert`). `ProfilesConfig` holds all profiles from a single `profiles.toml`. `ProjectSettings` holds the active profile name, optional Materialize version/Docker image override, and an optional `dependencies` array of fully-qualified `database.schema.object` names the project reads from but does not own.

`ProfilesConfig::expand_env_vars` resolves `${VAR}` inline references in the password field and falls back to `MZ_PROFILE_<NAME>_PASSWORD`.

`SecurityConfig` holds AWS profile settings for secret resolution. `PerProfileSettings` holds per-profile name suffix, security config, and psql-style variables resolved in SQL files before parsing.
