---
source: src/compute/src/logging/initialize.rs
revision: 8fd5a2a41a
---

# mz-compute::logging::initialize

Entry point for logging setup: `initialize` registers all Timely, differential, reachability, and compute loggers with the worker, then constructs the single combined logging dataflow that exports all log variants as `TraceBundle`s.
The order of logger registration vs. dataflow construction is controlled by `LoggingConfig::log_logging`, allowing self-logging of the logging dataflow itself.
Reachability loggers are registered with unique per-index links to avoid data loss, since `EventLink` is not multi-producer safe.
Returns `LoggingTraces` bundling the trace map, the logging dataflow index, and the compute logger handle.
