---
source: src/mz/src/sql_client.rs
revision: 4b9700a8af
---

# mz::sql_client

Provides the `Client` type that wraps an app password and constructs `psql` subprocess commands.
`shell` builds a `psql` URL from `RegionInfo`, configures SSL, sets `PGPASSWORD`/`PGAPPNAME`/`PSQLRC`, and returns a `Command` ready for execution.
`is_ready` runs `pg_isready` to check environment health.
`configure_psqlrc` creates `~/.psqlrc-mz` with `\timing` and `\include ~/.psqlrc` if it does not exist.
