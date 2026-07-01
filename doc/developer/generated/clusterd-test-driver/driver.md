---
source: src/clusterd-test-driver/src/driver.rs
revision: 141cb2a0a5
---

# mz-clusterd-test-driver::driver

The headless `Driver`: the mechanism's top-level API for test use cases.

`Driver` holds a `PersistHost`, the compute address string, a `ComputeSender`, and a `Responses` handle. It exposes:

* `connect(host, compute_addr)` — connects and sends `Hello`; spawns the response pump.
* `reconnect()` — drops the current connection and opens a new one with only `Hello`, so the caller can re-drive the controller handshake for reconnection testing.
* Controller handshake methods: `create_instance`, `update_configuration`, `initialization_complete`.
* Dataflow and data operations: `create_dataflow`, `drop_dataflow`, `allow_compaction`, `peek`, `cancel_peek`, `create_subscribe`, `allow_subscribe_compaction`.
* Frontier and peek waiters that delegate to `Responses`.

`create_instance` accepts an `initial_config: ConfigUpdates` argument — the create-time dyncfg snapshot the controller would supply — and force-disables `ENABLE_PEEK_RESPONSE_STASH` on top of it before sending `CreateInstance`. The stash disable is patched unconditionally so neither `initial_config` nor a later `update-configuration` can turn it back on, since the driver reads peek results inline.
