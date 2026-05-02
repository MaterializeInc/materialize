---
source: src/mz/src/command/app_password.rs
revision: 3ea5fbaaff
---

# mz::command::app_password

Implements the `mz app-password` subcommand: `create` calls the admin API to generate a new app password with a given description and prints it, while `list` retrieves and displays all existing app-password descriptions and creation dates via the output formatter.
Both operate on a `ProfileContext`.
