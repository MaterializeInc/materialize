---
source: src/ore/src/cli.rs
revision: 3538cae07a
---

# mz-ore::cli

Provides Materialize-specific wrappers around `clap` for consistent command-line argument parsing across binaries.

`parse_args` is the primary entry point: it accepts a `CliConfig` (optional env-var prefix, optional `--version` flag) and returns a fully-parsed `clap::Parser` implementation, injecting prefixed environment variable names and suppressing the version banner by default.
`KeyValueArg<K, V>` is a `FromStr` type for `KEY=VALUE` arguments, and `DefaultTrue` is a boolean flag that defaults to `true` but can be explicitly set to `false` via `--flag=false`.
