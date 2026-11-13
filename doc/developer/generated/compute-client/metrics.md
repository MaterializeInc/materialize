---
source: src/compute-client/src/metrics.rs
revision: 03de4421ba
---

# mz-compute-client::metrics

`CommandMetrics` provides a keyed metric set for each `ComputeCommand` variant.
Compute workers use it to measure command processing. It does not retain commands
or provide replay state.
