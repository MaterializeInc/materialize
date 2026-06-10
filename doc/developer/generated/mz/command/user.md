---
source: src/mz/src/command/user.rs
revision: 3ea5fbaaff
---

# mz::command::user

Implements the `mz user` subcommand: `create` adds a new user to the profile organization (assigning all existing roles), `list` displays all users, and `remove` removes a user by email—all via the admin (Frontegg) API client.
