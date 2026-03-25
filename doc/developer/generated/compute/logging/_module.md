---
source: src/compute/src/logging.rs
revision: e79a6d96d9
---

# mz-compute::logging

Provides the infrastructure for all introspection logging dataflows in the compute layer: Timely, differential, reachability, and compute-specific events.
`BatchLogger` bridges runtime event callbacks to `EventLink`-backed streams with millisecond-granularity progress; `EventQueue` bundles links with an `RcActivator`.
`PermutedRowPacker` packs datum slices into key/value row pairs according to a `LogVariant`'s index columns; `consolidate_and_pack` consolidates a stream worker-locally before row encoding.
The `initialize` submodule wires everything together; `compute`, `differential`, `reachability`, and `timely` each implement one fragment of the combined logging dataflow.
