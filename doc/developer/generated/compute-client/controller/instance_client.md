---
source: src/compute-client/src/controller/instance_client.rs
revision: b55d3dee25
---

# mz-compute-client::controller::instance_client

Provides `InstanceClient`, the public interface through which external callers (e.g., the adapter) communicate with a compute instance task over channels.
Commands are sent as boxed closures (`Command = Box<dyn FnOnce(&mut Instance) + Send>`) over an `mpsc` channel; `call` is fire-and-forget while `call_sync` awaits a `oneshot` response. Read hold changes are propagated via a separate `read_holds::ChangeTx<Timestamp>` sender.
`spawn` creates the `Instance` task and returns a client handle. Exposes operations for peek sequencing, acquiring read holds, and querying collection write frontiers; `pub(super)` methods serve the `ComputeController`'s internal use.
Also defines `PeekError`, `AcquireReadHoldsError`, and `InstanceShutDown` specific to this client boundary.
