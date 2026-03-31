---
source: src/compute/src/logging/timely.rs
revision: c642b63c77
---

# mz-compute::logging::timely

Constructs the Timely logging dataflow fragment, demuxing `TimelyEvent`s into ten separate collections: operator metadata (`Operates`, `Addresses`), channels, parks, message/batch counts, operator schedule durations, and a schedule-duration histogram.
On operator shutdown, the demux handler retracts the operator's metadata, schedule data, and all associated channel message counts to keep the logged collections consistent.
Dataflow-level shutdown events are forwarded to the compute logger via `SharedLoggingState` to trigger downstream retraction of arrangement heap-size operator records.
