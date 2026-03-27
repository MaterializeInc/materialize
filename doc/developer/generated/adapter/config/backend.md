---
source: src/adapter/src/config/backend.rs
revision: aa7a1afd31
---

# adapter::config::backend

Defines `SystemParameterBackend`, a client that pushes and pulls `SynchronizedParameters` to/from the coordinator by opening an internal system-user `SessionClient` and issuing `ALTER SYSTEM SET` statements.
It serves as the write side of the parameter-sync loop: pulling current values before pushing changes prevents overwriting concurrent modifications.
The internal session now includes an `AuthenticatorKind::None` marker in its `SessionConfig`.
