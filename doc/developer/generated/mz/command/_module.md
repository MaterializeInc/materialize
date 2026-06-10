---
source: src/mz/src/command.rs
revision: c95104d334
---

# mz::command

Module that re-exports the seven command implementation submodules: `app_password`, `config`, `profile`, `region`, `secret`, `sql`, and `user`.
Each submodule contains the actual logic for the corresponding `mz` CLI subcommand, operating on one of the three context types from `mz::context`.
