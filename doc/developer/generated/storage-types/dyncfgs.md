---
source: src/storage-types/src/dyncfgs.rs
revision: f2656c001e
---

# storage-types::dyncfgs

Declares `mz_dyncfg::Config` constants for all storage-layer dynamic configuration parameters.
Covers flow-control (backpressure, source suspension), controller behaviour (shard finalisation), Kafka client settings (reconnect/retry backoff, client-ID enrichment, PrivateLink endpoint ID algorithm), and statistics retention windows.
These constants are registered into a `ConfigSet` and can be read both statically during dataflow rendering and dynamically at runtime.
