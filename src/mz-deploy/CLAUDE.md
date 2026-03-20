# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What is mz-deploy

mz-deploy is a CLI deployment tool for Materialize. It compiles a directory of `.sql` files into a deployment plan, diffs it against the live environment, and executes blue/green schema migrations via Materialize's zero-downtime deployment primitives.

## Build & Test Commands

mz-deploy is a Rust crate within the Materialize workspace (root at `../../`). All cargo commands must be run from the workspace root or use `--manifest-path`.

```sh
# Build (from workspace root)
cargo build -p mz-deploy

# Run unit tests
cargo test -p mz-deploy

# Run a single test
cargo test -p mz-deploy -- test_name

# Format
cargo fmt -p mz-deploy

# Clippy
cargo clippy -p mz-deploy

# Run the binary (after building)
cargo run -p mz-deploy -- <args>
# or directly:
../../target/debug/mz-deploy <args>
```

### E2E tests (mzcompose)

Integration tests live in `../../test/mz-deploy/` and use the mzcompose framework. They spin up a real Materialized instance and run mz-deploy against test projects in `test/mz-deploy/projects/`.

```sh
# From repo root
./bin/mzcompose --find mz-deploy run default
```

## Architecture

Four major layers, each in its own module under `src/`:

### `project` — SQL Pipeline (`raw` → `typed` → `planned`)
The core compilation pipeline. SQL files on disk go through three stages:
1. **`raw`** — Load `.sql` files, parse into AST. No semantic checks.
2. **`typed`** — Validate naming, one-main-statement-per-file, cross-references (indexes, grants, comments). Produces typed IR.
3. **`planned`** — Extract dependency graphs, topological sort, produce final deployment plan.

Entry point: `project::plan()` runs the full pipeline.

Sibling modules handle deployment analysis:
- **`changeset`** — Datalog-style dirty propagation: compares project against prior snapshot, computes which objects/schemas/clusters need redeployment via fixed-point iteration.
- **`normalize`** — AST rewriting for blue/green (e.g., prefixing schema names with deploy ID).
- **`deployment_snapshot`** — Hash-based change detection so formatting-only changes don't trigger redeployment.

### `client` — Database Client
All live database interaction. Sub-clients group operations:
- `introspection` — Read-only catalog queries
- `provisioning` — DDL for databases, schemas, clusters
- `deployment_ops` — Blue/green lifecycle (stage, hydration monitoring, cutover, abort)
- `validation` — Pre-deployment environment checks
- `type_info` — `SHOW COLUMNS` for `types.lock` generation

### `cli` — Command-Line Interface
- `commands/` — One module per subcommand with a `run()` entry point
- `executor` — Command lifecycle orchestration (config loading, connection setup, dispatch)
- `extended_help` — Rich help text loaded from markdown files in `src/cli/help/`

### `types` — Data Contract System
Manages `types.lock` (column schemas for external dependencies) and `types.cache` (internal view schemas after type-checking). The `TypeChecker` trait validates SQL against a real Materialize Docker container.

## Change Strategy

When a change involves both restructuring code and adding new behavior (like parallelism), split into independent steps: first refactor the structure while keeping behavior identical, validate that step, then layer on the new behavior.

## Key Conventions

- **No stdout/stderr in library code**: `#![deny(clippy::print_stdout)]` and `#![deny(clippy::print_stderr)]` are enforced. For command output, use `log::output()` with a type that implements `Render` (i.e., both `Display` and `Serialize`) — this automatically handles human text (stderr) vs JSON (stdout) based on the `--output` flag. Use `info!()` for supplementary stderr messages (hints, progress) that shouldn't appear in JSON output. Use `verbose!()` for debug-level diagnostic logging that only appears with `--verbose`.
- **Error handling**: Each layer has its own error module. CLI errors use `CliError` with optional `hint()` for user-facing messages rendered in rustc style.
- **Functional core, imperative shell**: When a function mixes decision logic with I/O (database calls, file operations, network), extract the decisions into a pure struct or function and push I/O to a thin caller loop. The pure core takes inputs, returns actions or updated state, and is trivially unit-testable without mocks or async runtimes. The async/IO wrapper becomes a simple dispatch loop that executes the core's decisions. Cover the core's logic thoroughly with unit tests — classification, state transitions, edge cases, and output merging should all have dedicated test cases. See `DirtyPropagator` in `src/types/typechecker.rs` for the canonical example.
- **Subcommand help**: Extended help is in markdown files at `src/cli/help/*.md`. The `GROUPED_HELP` constant in `main.rs` must stay in sync with `Command` variants (enforced by a test).
- **Config resolution**: `profiles.toml` (connection details) lives in `~/.mz/` or `--profiles-dir`. `project.toml` lives in the project root. Both are loaded into `Settings` which is passed to all commands.
- **Feature flags**: `vendored-openssl` (for release builds).
- **Formatting**: Run `cargo fmt -p mz-deploy` after making changes to ensure consistent code style.
- **Documentation**: All `pub` modules, types, and functions must have doc comments. See the **Specification Documentation** section below.

## Specification Documentation

Module and function doc comments are the canonical specification of what the software does. They describe behavior, rules, invariants, and contracts — not implementation details. These specs are the primary way developers (and Claude) understand the codebase. Before modifying a module, read its `//!` doc comment. After changing behavior, update the spec. Treat stale specs as bugs.

### Structure Guide

Follow these patterns when writing specification doc comments:

- **Purpose statement** — Single sentence at the top of `//!` describing the module's responsibility.
- **Algorithm / Rules section** — For modules with non-trivial logic: named result sets, Datalog-style rules, transformation tables, or validation rule lists.
- **Key insights / invariants** — Bold `**Key Insight:**` or `**Note:**` callouts for edge cases and design decisions that would surprise a reader.
- **Pipeline integration** — How the module fits into the larger system, with ASCII flow diagrams where helpful.
- **Function-level specs** — For important public functions: what it computes, argument semantics, return value meaning, and concrete examples.

### Workflow

- Before modifying a module, read its `//!` doc comment to understand the specification
- After modifying behavior, update the doc comment to reflect the new specification
- When creating a new module, write the specification doc comment first
- The spec should stay in sync with the code — treat stale specs as bugs
- **Plans must include spec updates**: When creating an implementation plan, explicitly call out which module and function doc comments need to be added or updated. Spec changes are part of the deliverable, not an afterthought.
