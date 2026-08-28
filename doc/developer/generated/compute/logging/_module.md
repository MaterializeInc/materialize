---
source: src/compute/src/logging.rs
revision: 780c9c1add
---

# mz-compute::logging

Provides the infrastructure for all introspection logging dataflows in the compute layer: Timely, differential, reachability, compute-specific events, Prometheus metrics, and resource usage.
`BatchLogger` bridges runtime event callbacks to `EventLink`-backed streams with millisecond-granularity progress; `EventQueue` bundles links with an `RcActivator` (parameterized by `N` links for multi-producer safety).
`PermutedRowPacker` packs datum slices into key/value row pairs according to a `LogVariant`'s index columns. `SharedLoggingState` holds arrangement-size activators and the compute logger shared across dataflow fragments.
Two shared utilities are exported to logging dataflow fragments: `downgrade_to_interval_boundary` advances a `Capability<Timestamp>` to the next logging-interval boundary and schedules the next activation there, keeping the output frontier progressing at the logging rate without drifting from wall-clock elapsed time; `emit_snapshot_diff` diffs two `BTreeMap` snapshots of a sampled source and emits the changed key/value pairs as retractions and insertions at a given timestamp.
The `initialize` submodule wires everything together; `compute`, `differential`, `reachability`, `timely`, `prometheus`, and `resource_usage` each implement one fragment of the combined logging dataflow.

## Submodules

- `compute` -- compute-specific event logging (dataflows, frontiers, peeks, arrangement sizes, error counts, hydration).
- `differential` -- differential dataflow event logging (arrangement batches, records, sharing, batcher stats).
- `initialize` -- entry point that registers loggers and constructs the combined logging dataflow.
- `prometheus` -- Prometheus metrics scraping exposed as an introspection source.
- `reachability` -- reachability tracker event logging.
- `resource_usage` -- resource usage observations of each replica process exposed as an introspection source.
- `timely` -- Timely runtime event logging (operators, channels, scheduling, messages).
