---
source: src/mz-deploy/src/bin/mz-deploy/main.rs
revision: 8ee3def844
---

# mz-deploy (binary entry point)

Binary entry point for the `mz-deploy` CLI.

Parses CLI arguments via `clap`, initializes logging, loads project and profile configuration via `Settings::load`, and dispatches to the appropriate command handler in `mz_deploy::cli::commands`. Errors are rendered to stderr with rustc-style colored formatting via `cli::display_error`; in JSON output mode they are serialized and written to stdout instead.

## Commands

Commands are grouped into four areas:

* **Getting started**: `new`, `init`, `profile`, `setup`, `debug`
* **Develop**: `compile`, `clean`, `test`, `explain`, `dev`, `lsp`, `sql`, `mcp`
* **Infrastructure**: `lock`, `apply`, `delete`
* **Deploy**: `stage`, `wait`, `promote`, `abort`, `describe`, `list`, `log`

Global flags (`--directory`, `--profile`, `--verbose`, `--quiet`, `--output`) are defined on `GlobalArgs` and marked `global = true` so they apply to all subcommands.
