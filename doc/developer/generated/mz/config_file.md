---
source: src/mz/src/config_file.rs
revision: f38003ddc8
---

# mz::config_file

Manages the `~/.config/materialize/mz.toml` TOML configuration file.
`ConfigFile` wraps both a parsed representation and an editable `DocumentMut` to support non-destructive in-place updates; exposes methods to load/add/remove profiles, and to get/set global parameters (`profile`, `vault`) and per-profile parameters (`app-password`, `region`, `admin-endpoint`, `cloud-endpoint`).
On macOS, supports the `Keychain` vault variant to store app passwords in the system keychain instead of in the config file.
