---
source: src/mz-deploy/src/cli/commands.rs
revision: b0390d141f
---

# mz-deploy::cli::commands

One module per CLI subcommand. Each module exposes a `run()` function that is the entry point for that subcommand. Subcommands include: `abort`, `apply_all`, `apply_connections`, `apply_network_policies`, `apply_objects`, `apply_secrets`, `apply_sources`, `apply_tables`, `clean`, `clusters`, `compile`, `debug`, `delete`, `describe`, `dev`, `explain`, `grants`, `list`, `lock`, `log`, `mcp`, `new_project`, `profile`, `promote`, `roles`, `setup`, `setup_schema`, `sql`, `stage`, `test`, `wait`.
