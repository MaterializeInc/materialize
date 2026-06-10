---
source: src/sql/src/session/vars/errors.rs
revision: a36c8a4b62
---

# mz-sql::session::vars::errors

Defines `VarError` and `VarParseError`, the error types returned when setting or parsing session/system variables (constrained value, fixed value, wrong type, read-only, feature flag disabled, etc.).
`VarError` implements `thiserror::Error` with human-readable messages matching PostgreSQL error format.
