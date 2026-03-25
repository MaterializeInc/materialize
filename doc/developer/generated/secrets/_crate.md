---
source: src/secrets/src/lib.rs
revision: 235d12a7d9
---

# mz-secrets

Defines the `SecretsController` and `SecretsReader` traits for secure management of user secrets identified by `CatalogItemId`.
`InMemorySecretsController` provides a BTreeMap-backed in-process implementation used in tests.
`CachingPolicy` and the `cache` submodule add a TTL-based caching layer over any `SecretsReader`.
Concrete production implementations live in `mz-orchestrator-process` (filesystem) and other orchestrator crates.

## Module structure

* `cache` — TTL-based `CachingSecretsReader`
