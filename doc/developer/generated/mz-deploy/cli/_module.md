---
source: src/mz-deploy/src/cli.rs
revision: a647094cc4
---

# mz-deploy::cli

Command-line interface for mz-deploy.

`display_error` renders a `CliError` to stderr with rustc-style colored formatting and optional hints. For errors with source positions it uses `annotate-snippets` to display a caret under the offending token.

Submodules:
* `commands` — one module per CLI subcommand, each exposing a `run()` entry point.
* `executor` — orchestrates the full command lifecycle: loads configuration, establishes database connections, dispatches to the appropriate command module.
* `error` — `CliError` enum unifying all user-facing errors with optional hints.
* `extended_help` — extended help text for subcommands.
* `git` — git integration helpers (e.g. detecting the project root).
* `progress` — progress indicator for long-running operations.
* `render` — positional diagnostic rendering for source-annotated errors.
