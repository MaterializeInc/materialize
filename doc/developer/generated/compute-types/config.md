---
source: src/compute-types/src/config.rs
revision: 56b1bb68fc
---

# compute-types::config

Defines `ComputeReplicaConfig` and `ComputeReplicaLogging`, the configuration types for a compute replica.
`ComputeReplicaLogging` controls whether introspection logging is enabled and the sampling interval; a `None` interval disables logging entirely.
When logging is disabled, 0dt cutovers still wait for replica hydration but not for the stability period, so a crash-looping replica can pass the cutover check.
