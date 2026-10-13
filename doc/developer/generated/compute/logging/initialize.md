---
source: src/compute/src/logging/initialize.rs
revision: cdf204afe7
---

# mz-compute::logging::initialize

Entry point for logging setup: `initialize` registers all Timely, differential, reachability, and compute loggers with the worker, then constructs the single combined logging dataflow that exports all log variants as `TraceBundle`s. It accepts a `LoggingMetrics` argument (obtained from `WorkerMetrics::for_logging`) that carries per-log record counters and a step-duration histogram passed through to the loggers and the step-timing logger.
The `LoggingContext` struct holds event queues, shared state, the metrics registry, per-worker `LoggingMetrics`, and worker configuration. `construct_dataflow` wires together the timely, reachability, differential, compute, prometheus, and resource_usage dataflow fragments, arranging each log collection into a `TraceBundle`.
The error collection for the logging dataflow uses `DataflowErrorSer` as its error type, consistent with the rest of the compute layer.
The order of logger registration vs. dataflow construction is controlled by `LoggingConfig::log_logging`, allowing self-logging of the logging dataflow itself.
Returns `LoggingTraces` bundling the trace map, the logging dataflow index, and the compute logger handle.
