---
source: src/mz/src/lib.rs
revision: e757b4d11b
---

# mz

`mz` is the Materialize command-line interface, providing subcommands for managing cloud environments, authentication profiles, app passwords, regions, users, secrets, and SQL access.

Library modules:
* `command` — implementations of each subcommand (`app_password`, `config`, `profile`, `region`, `secret`, `sql`, `user`)
* `config_file` — TOML configuration file management with optional macOS keychain vault
* `context` — three-level context hierarchy (`Context`, `ProfileContext`, `RegionContext`)
* `error` — unified `Error` enum
* `server` — ephemeral local HTTP server for OAuth browser login callback
* `sql_client` — `psql`-based SQL shell client
* `ui` — `OutputFormatter` supporting text/JSON/CSV output

The binary in `src/bin/mz/` handles clap argument parsing and dispatches to the library.
Key dependencies: `mz-cloud-api`, `mz-frontegg-client`, `mz-frontegg-auth`, `mz-ore`, `mz-sql-parser`.
