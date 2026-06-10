---
source: src/storage/src/statistics.rs
revision: c8a90af6a2
---

# mz-storage::statistics

Implements the statistics collection and aggregation layer for sources and sinks.
Defines `SourceStatisticsMetricDefs`, `SourceStatisticsRecord`, `SinkStatisticsMetricDefs`, and `SinkStatisticsRecord`, which track user-visible statistics (messages received, bytes, updates staged/committed, snapshot state, offset progress) and simultaneously maintain Prometheus counters/gauges.
Statistics are aggregated across workers with `AggregatedStatistics` and periodically shipped to the controller via `StatisticsUpdate` internal commands.
Prometheus metrics intentionally do not share the same semantic guarantees as the user-visible statistics.
All metric vectors carry enrichment `Rule`s: source metrics use `source_name_rule` (`ObjectNameLookup` on `source_id`) and, where a `parent_source_id` label is present, `parent_source_name_rule` (which writes distinct `parent_source_name`, `parent_source_schema_name`, and `parent_source_database_name` labels to avoid colliding with the primary source rule's output). Sink metrics use `sink_name_rule` (`ObjectNameLookup` on `sink_id`).
