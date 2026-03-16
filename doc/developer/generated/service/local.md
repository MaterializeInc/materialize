---
source: src/service/src/local.rs
revision: 82d92a7fad
---

# mz-service::local

Provides `LocalClient`, a `GenericClient` implementation backed by Tokio unbounded MPSC channels that communicates with a worker thread in the same process.
The thread is unparked on every `send` and on `Drop` to ensure the worker wakes up.
`LocalClient::new_partitioned` constructs a `Partitioned<LocalClient, C, R>` from parallel channel sets.
