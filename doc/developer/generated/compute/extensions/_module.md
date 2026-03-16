---
source: src/compute/src/extensions.rs
revision: d546d1b190
---

# mz-compute::extensions

Groups Materialize-specific extensions to Timely and differential dataflow operators.
`arrange` wraps arrangement creation with heap-size logging; `reduce` wraps reductions to ensure the same logging; `temporal_bucket` provides bucket-chain-based temporal delay for future-timestamped data.
These extensions enforce consistent observability and correctness policies across all operator creation sites in the compute layer.
