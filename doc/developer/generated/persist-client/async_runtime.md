---
source: src/persist-client/src/async_runtime.rs
revision: a5d2cc69e4
---

# persist-client::async_runtime

Provides `IsolatedRuntime`, a dedicated Tokio multi-thread runtime for persist's CPU-intensive and potentially long-running tasks (encoding, decoding, shard maintenance).
Running persist work on separate OS threads prevents misbehaving tasks from starving the main process scheduler, as opposed to using `spawn_blocking` which shares a thread pool and can cause shutdown hazards.
