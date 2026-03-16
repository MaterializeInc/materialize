---
source: src/secrets/src/cache.rs
revision: eb687f8c0b
---

# mz-secrets::cache

Provides `CachingSecretsReader`, a `SecretsReader` wrapper that caches secret bytes in memory with a configurable TTL (default 300 s).
Cache policy (enabled/disabled, TTL) is stored in `CachingParameters` using atomics so it can be updated at runtime without locking.
On a cache miss or expiry the reader falls through to the underlying `SecretsReader`; disabling the cache also clears all stored values immediately.
