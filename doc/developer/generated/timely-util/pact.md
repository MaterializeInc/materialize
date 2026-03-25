---
source: src/timely-util/src/pact.rs
revision: f641a29d4a
---

# timely-util::pact

Defines `Distribute`, a `ParallelizationContract` that routes containers to workers in round-robin order, and its associated `DistributePusher<P>`.
This is more efficient than `Exchange` when the target worker is irrelevant and load balancing by rotation is sufficient.
