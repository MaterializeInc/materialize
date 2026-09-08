---
source: src/mz/src/command/profile.rs
revision: fb86c94c64
---

# mz::command::profile

Implements the `mz profile` subcommand: initializing a profile via browser-based OAuth login (`init_with_browser`) or interactive credential prompting, listing profiles, removing profiles, and showing/updating profile parameters.
During `init`, writability of the configuration file is verified before the OAuth/credential flow begins, so an unwritable config file does not leave behind an app password that no profile records.
Manages the relationship between the configuration file and the macOS keychain for app-password storage.
