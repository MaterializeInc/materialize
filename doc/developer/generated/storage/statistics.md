---
source: src/storage/src/statistics.rs
revision: ca79be2bb2
---

# mz-storage::statistics

Implements the statistics collection and aggregation layer for sources and sinks.
Defines `SourceStatisticsMetricDefs`, `SourceStatisticsRecord`, `SinkStatisticsMetricDefs`, and `SinkStatisticsRecord`, which track user-visible statistics (messages received, bytes, updates staged/committed, snapshot state, offset progress) and simultaneously maintain Prometheus counters/gauges.
Statistics are aggregated across workers with `AggregatedStatistics` and periodically shipped to the controller via `StatisticsUpdate` internal commands.
Prometheus metrics intentionally do not share the same semantic guarantees as the user-visible statistics.
