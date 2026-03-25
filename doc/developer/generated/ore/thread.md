---
source: src/ore/src/thread.rs
revision: 17709bdab4
---

# mz-ore::thread

Provides RAII wrappers for `std::thread::JoinHandle` with two drop behaviors.
`JoinOnDropHandle<T>` joins (and propagates panics from) the child thread when dropped; `UnparkOnDropHandle<T>` unparks the child thread when dropped, leaving it to detach normally.
`JoinHandleExt` adds `join_on_drop` and `unpark_on_drop` conversion methods directly to `JoinHandle<T>`.
