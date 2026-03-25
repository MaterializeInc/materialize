---
source: src/compute-client/src/controller/instance_client.rs
revision: 681cdf1339
---

# mz-compute-client::controller::instance_client

Provides `InstanceClient`, the public interface through which external callers (e.g., the adapter) communicate with a compute instance task over channels.
Exposes operations for peek sequencing, acquiring read holds, and querying collection write frontiers; `pub(super)` methods serve the `ComputeController`'s internal use.
Also defines `PeekError` and `AcquireReadHoldsError` specific to this client boundary.
