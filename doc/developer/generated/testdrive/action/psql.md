---
source: src/testdrive/src/action/psql.rs
revision: e757b4d11b
---

# testdrive::action::psql

Implements the `psql-execute` builtin command, which spawns the `psql` CLI to run a command against Materialize and compares stdout output against expected lines.
Uses `--no-psqlrc` and `--pset footer=off` to ensure deterministic output regardless of the local environment.
