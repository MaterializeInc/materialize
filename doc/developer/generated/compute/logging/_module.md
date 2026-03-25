---
source: src/compute/src/logging.rs
revision: f94584ddef
---

# mz-compute::logging

Provides the infrastructure for all introspection logging dataflows in the compute layer: Timely, differential, reachability, compute-specific events, and Prometheus metrics.
`BatchLogger` bridges runtime event callbacks to `EventLink`-backed streams with millisecond-granularity progress; `EventQueue` bundles links with an `RcActivator` (parameterized by `N` links for multi-producer safety).
`PermutedRowPacker` packs datum slices into key/value row pairs according to a `LogVariant`'s index columns. `SharedLoggingState` holds arrangement-size activators and the compute logger shared across dataflow fragments.
The `initialize` submodule wires everything together; `compute`, `differential`, `reachability`, `timely`, and `prometheus` each implement one fragment of the combined logging dataflow.

## Submodules

- `compute` -- compute-specific event logging (dataflows, frontiers, peeks, arrangement sizes, error counts, hydration).
- `differential` -- differential dataflow event logging (arrangement batches, records, sharing, batcher stats).
- `initialize` -- entry point that registers loggers and constructs the combined logging dataflow.
- `prometheus` -- Prometheus metrics scraping exposed as an introspection source.
- `reachability` -- reachability tracker event logging.
- `timely` -- Timely runtime event logging (operators, channels, scheduling, messages).
