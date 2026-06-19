---
source: src/mz-deploy/src/lib.rs
revision: a647094cc4
---

# mz-deploy

Safe, testable deployments for Materialize. `mz-deploy` compiles a directory of `.sql` files into a deployment plan, diffs it against the live environment, and executes blue/green schema migrations via Materialize's zero-downtime deployment primitives.

## Architecture

Four major layers:

* **`cli`** — Command-line interface: argument parsing, subcommand dispatch, error formatting. `display_error` renders `CliError` with rustc-style colored output.
* **`client`** — Database client layer: connection management, introspection queries, DDL provisioning, blue/green deployment lifecycle, and type-info queries against a live Materialize region.
* **`project`** — Project compiler: loads `.sql` files, validates, type-checks, resolves dependencies, and produces a deployment graph.
* **`types`** — Data-contract system: `types.lock` file pinning column schemas for external dependencies.

## Supporting modules

* `config` — Profile and project settings loading from `profiles.toml` and `project.toml`. `Profile` holds resolved connection details; `ProjectSettings` holds active profile, version override, and dependencies.
* `diagnostics` — Diagnostic reporting helpers.
* `docker_runtime` — Docker image management for local dev (spinning up a Materialize instance for type-checking).
* `fs` — Filesystem utilities.
* `log` — Verbose logging and the `verbose!` macro.
* `lsp` — Language Server Protocol server for IDE integration: completion, hover, go-to-definition, references, diagnostics, code lens, document symbols, and workspace symbols.
* `secret_resolver` — Secret resolution from AWS Secrets Manager, environment variables, and JSON field extraction.

## Key dependencies

`tokio-postgres`, `postgres-openssl`, `mz-sql-parser`, `tower-lsp`, `aws-sdk-secretsmanager`, `annotate-snippets`, `owo-colors`.
