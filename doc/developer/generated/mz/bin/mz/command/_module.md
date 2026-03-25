---
source: src/mz/src/bin/mz/command.rs
revision: c95104d334
---

# mz (bin)::command

Re-exports all seven clap command-driver submodules (`app_password`, `config`, `profile`, `region`, `secret`, `sql`, `user`).
Each submodule contains only clap struct definitions and a thin `run` dispatcher that delegates to the library crate.
