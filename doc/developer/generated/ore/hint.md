---
source: src/ore/src/hint.rs
revision: 0f7a9b2733
---

# mz-ore::hint

Provides a stable-compatible `black_box<T>(dummy: T) -> T` function derived from the criterion benchmarking crate.
It uses `read_volatile` and `mem::forget` to prevent the compiler from optimizing away benchmark computations, serving as a stand-in for `std::hint::black_box` on older toolchains.
