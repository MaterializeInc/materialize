---
source: src/adapter/src/config/backend.rs
revision: 0a20c581ea
---

# adapter::config::backend

Defines `SystemParameterBackend`, a client that pushes and pulls `SynchronizedParameters` to/from the coordinator by opening an internal system-user `SessionClient` and issuing `ALTER SYSTEM SET` statements.
It serves as the write side of the parameter-sync loop: pulling current values before pushing changes prevents overwriting concurrent modifications.
The internal session now includes an `AuthenticatorKind::None` marker in its `SessionConfig`.
