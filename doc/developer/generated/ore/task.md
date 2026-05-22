---
source: src/ore/src/task.rs
revision: dec5c85cbb
---

# mz-ore::task

Provides wrappers around Tokio's task-spawning API that attach names to tasks and give callers infallible join handles.
`JoinHandle<T>` wraps `tokio::task::JoinHandle<T>` and implements `Future<Output = T>` directly (panicking on task panic, returning `Pending` on runtime shutdown cancellation); it exposes `abort_on_drop`, `abort_and_wait`, and `into_tokio_handle`.
`AbortOnDropHandle<T>` wraps `JoinHandle<T>` and aborts the underlying task when dropped.
`spawn` and `spawn_blocking` are replacements for the raw Tokio equivalents; both box the future as `Box::pin(future)` before passing it to `tokio::spawn`, so tokio's task machinery is monomorphized over `Pin<Box<dyn Future>>` per output type rather than over every distinct future type at every call site. When built with `tokio_unstable` they additionally set a structured task name via `task::Builder`.
`RuntimeExt` and `JoinSetExt` extend `Runtime`, `Handle`, and `JoinSet` with `spawn_named` / `spawn_blocking_named` methods following the same naming convention.
