---
source: src/persist-client/src/cli/args.rs
revision: 181b1e7efc
---

# persist-client::cli

Contains the CLI subcommands exposed by `mz-persist-cli` and used internally: `inspect` (read-only state dumping), `admin` (state-mutating operations), and `bench` (throughput benchmarks).
Shared argument types in `args` handle storage connection setup for all subcommands.
