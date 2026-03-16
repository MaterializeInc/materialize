---
source: src/mz/src/command/profile.rs
revision: 96f48dd2ed
---

# mz::command::profile

Implements the `mz profile` subcommand: initializing a profile via browser-based OAuth login (`init_with_browser`) or interactive credential prompting, listing profiles, removing profiles, and showing/updating profile parameters.
Manages the relationship between the configuration file and the macOS keychain for app-password storage.
