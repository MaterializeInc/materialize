---
source: src/mz/src/error.rs
revision: fb86c94c64
---

# mz::error

Defines the `Error` enum covering all error variants produced by the `mz` CLI, including authentication errors, API errors, configuration parse errors, IO errors, a `ConfigFileNotWritable` variant for when the configuration file exists but cannot be written, timeout errors, and macOS keychain errors.
Provides user-friendly error messages with actionable guidance for the most common failure cases.
