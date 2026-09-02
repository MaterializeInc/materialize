---
source: src/ore/src/hint.rs
revision: 26f46d16d4
---

# mz-ore::hint

Provides a `black_box<T>(dummy: T) -> T` function that forwards directly to `std::hint::black_box`.
Retained for existing call sites; prefer calling `std::hint::black_box` directly.
