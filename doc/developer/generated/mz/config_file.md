---
source: src/mz/src/config_file.rs
revision: fb86c94c64
---

# mz::config_file

Manages the `~/.config/materialize/mz.toml` TOML configuration file.
`ConfigFile` wraps both a parsed representation and an editable `DocumentMut` to support non-destructive in-place updates; exposes methods to load/add/remove profiles, and to get/set global parameters (`profile`, `vault`) and per-profile parameters (`app-password`, `region`, `admin-endpoint`, `cloud-endpoint`).
`load` treats a missing file as an empty configuration and never creates the file or its parent directory, so read-only operations work when `mz.toml` lives on a read-only mount.
`ensure_writable` verifies that the configuration file can be written (creating the file and parent directory if missing) and must be called by any command that causes external side effects before writing the configuration file.
On macOS, supports the `Keychain` vault variant to store app passwords in the system keychain instead of in the config file.
