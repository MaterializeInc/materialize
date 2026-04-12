---
source: src/compute/src/logging/initialize.rs
revision: 8864f35b2c
---

# mz-compute::logging::initialize

Entry point for logging setup: `initialize` registers all Timely, differential, reachability, and compute loggers with the worker, then constructs the single combined logging dataflow that exports all log variants as `TraceBundle`s.
The `LoggingContext` struct holds event queues, shared state, the metrics registry, and worker configuration. `construct_dataflow` wires together the timely, reachability, differential, compute, and prometheus dataflow fragments, arranging each log collection into a `TraceBundle`.
The order of logger registration vs. dataflow construction is controlled by `LoggingConfig::log_logging`, allowing self-logging of the logging dataflow itself.
Returns `LoggingTraces` bundling the trace map, the logging dataflow index, and the compute logger handle.
