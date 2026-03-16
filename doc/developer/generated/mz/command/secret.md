---
source: src/mz/src/command/secret.rs
revision: d7d708372c
---

# mz::command::secret

Implements the `mz secret create` command, which reads a secret value from stdin and executes the appropriate `CREATE SECRET` (or `CREATE SECRET IF NOT EXISTS` + `ALTER SECRET` for upsert) SQL via `psql`.
Handles both raw string literals and `decode(...)` expressions.
