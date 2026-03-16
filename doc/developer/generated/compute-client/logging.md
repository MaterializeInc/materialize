---
source: src/compute-client/src/logging.rs
revision: 4267863081
---

# mz-compute-client::logging

Defines logging configuration and introspection log variants for the compute layer.
`LoggingConfig` controls whether logging is enabled, the sampling interval, and which log indexes are installed.
`LogVariant` enumerates the three log families — `TimelyLog`, `DifferentialLog`, and `ComputeLog` — and provides `desc()` to return the `RelationDesc` for each variant, which must match the schema expected by the catalog and the logging dataflows.
