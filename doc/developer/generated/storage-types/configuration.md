---
source: src/storage-types/src/configuration.rs
revision: a375623c5b
---

# storage-types::configuration

Defines `StorageConfiguration`, the top-level configuration struct that bundles all parameters required for interacting with storage APIs.
It combines `StorageParameters` (LD-controlled, serializable, sent from `environmentd` to `clusterd`) with an immutable `ConnectionContext` and a shared `ConfigSet`.
The `update` method applies incoming `StorageParameters` to both the parameter field and the dynamic config set atomically.
