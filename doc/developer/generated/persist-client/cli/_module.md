---
source: src/persist-client/src/cli/args.rs
revision: 4a1aeff959
---

# persist-client::cli

Contains the CLI subcommands exposed by `mz-persist-cli` and used internally: `inspect` (read-only state dumping), `admin` (state-mutating operations), and `bench` (throughput benchmarks).
Shared argument types in `args` handle storage connection setup for all subcommands.
