---
source: src/mz/src/bin/mz/command/profile.rs
revision: 5680493e7d
---

# mz (bin)::command::profile

Clap argument parser and dispatcher for `mz profile`, with top-level subcommands `Init`, `List`, `Remove`, and `Config`.
`Config` is itself a subcommand group with `Get`, `List`, `Set`, and `Remove` sub-subcommands for reading and writing individual profile configuration parameters.
Delegates implementation to `mz::command::profile`.
