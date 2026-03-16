---
source: src/environmentd/src/telemetry.rs
revision: 94bfc93de1
---

# environmentd::telemetry

Runs a periodic reporting loop that queries the adapter for environment statistics (active sources, sinks, views, clusters, etc.) and sends them to Segment via `group` (latest state) and `track` ("Environment Rolled Up" event with delta statistics).
The loop starts via `start_reporting` and fires at a configurable `report_interval`; the first tick only sends traits, subsequent ticks also compute and report delta counters for DML operations.
