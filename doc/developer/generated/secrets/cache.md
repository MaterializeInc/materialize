---
source: src/secrets/src/cache.rs
revision: a29f0a64ed
---

# mz-secrets::cache

Provides `CachingSecretsReader`, a `SecretsReader` wrapper that caches secret bytes in memory with a configurable TTL (default 300 s).
Cache policy (enabled/disabled, TTL) is stored in `CachingParameters` using atomics so it can be updated at runtime without locking.
On a cache miss or expiry the reader falls through to the underlying `SecretsReader`; disabling the cache also clears all stored values immediately.
The `invalidate` method allows removing a single cached secret by `CatalogItemId`, used when a secret is updated or deleted.
