---
source: src/ore/src/hash.rs
revision: 0f7a9b2733
---

# mz-ore::hash

Thin module providing a single `hash<T: Hash>(t: &T) -> u64` convenience function that hashes a value with `DefaultHasher` and returns the resulting `u64`.
