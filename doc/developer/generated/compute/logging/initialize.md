---
source: src/compute/src/logging/initialize.rs
revision: 44a09cff14
---

# mz-compute::logging::initialize

Entry point for logging setup: `initialize` registers all Timely, differential, reachability, and compute loggers with the worker, then constructs the single combined logging dataflow that exports all log variants as `TraceBundle`s.
`initialize` accepts an optional `StorageTimelyLogReader` which is threaded through `LoggingContext` into the timely logging fragment so that storage worker timely events are replayed and merged with compute timely events.
The `LoggingContext` struct holds event queues, shared state, the metrics registry, and worker configuration. `construct_dataflow` wires together the timely, reachability, differential, compute, and prometheus dataflow fragments, arranging each log collection into a `TraceBundle`.
The error collection for the logging dataflow uses `DataflowErrorSer` as its error type, consistent with the rest of the compute layer.
The order of logger registration vs. dataflow construction is controlled by `LoggingConfig::log_logging`, allowing self-logging of the logging dataflow itself.
Returns `LoggingTraces` bundling the trace map, the logging dataflow index, and the compute logger handle.
