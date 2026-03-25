---
source: src/compute-client/src/logging.rs
revision: f94584ddef
---

# mz-compute-client::logging

Defines logging configuration and introspection log variants for the compute layer.
`LoggingConfig` controls whether logging is enabled, the sampling interval (`interval`), self-logging (`log_logging`), and which log indexes are installed (`index_logs`).
`LogVariant` enumerates the three log families -- `TimelyLog` (operators, channels, scheduling, messages, reachability), `DifferentialLog` (arrangement batches, records, sharing, batcher stats), and `ComputeLog` (dataflows, frontiers, peeks, arrangement sizes, error counts, hydration, LIR mappings, Prometheus metrics) -- and provides `index_by()` and `desc()` to return the index columns and `RelationDesc` for each variant, which must match the schema expected by the catalog and the logging dataflows.
