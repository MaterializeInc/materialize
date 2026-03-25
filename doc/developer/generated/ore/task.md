---
source: src/ore/src/task.rs
revision: 8e6ee2a518
---

# mz-ore::task

Provides wrappers around Tokio's task-spawning API that attach names to tasks and give callers infallible join handles.
`JoinHandle<T>` wraps `tokio::task::JoinHandle<T>` and implements `Future<Output = T>` directly (panicking on task panic, returning `Pending` on runtime shutdown cancellation); it exposes `abort_on_drop`, `abort_and_wait`, and `into_tokio_handle`.
`AbortOnDropHandle<T>` wraps `JoinHandle<T>` and aborts the underlying task when dropped.
`spawn` and `spawn_blocking` are replacements for the raw Tokio equivalents; when built with `tokio_unstable` they set a structured task name via `task::Builder`.
`RuntimeExt` and `JoinSetExt` extend `Runtime`, `Handle`, and `JoinSet` with `spawn_named` / `spawn_blocking_named` methods following the same naming convention.
