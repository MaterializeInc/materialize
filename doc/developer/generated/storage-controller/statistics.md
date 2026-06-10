---
source: src/storage-controller/src/statistics.rs
revision: 00cc513fa5
---

# storage-controller::statistics

Provides two background Tokio tasks that periodically flush storage statistics into managed collections.
`spawn_statistics_scraper` consolidates per-source or per-sink statistics held in shared memory and emits differential updates to the corresponding introspection collection.
`spawn_webhook_statistics_scraper` drains atomic webhook counters into the shared `SourceStatistics` map so they are picked up by the main scraper.
The `SourceStatistics` wrapper and `AsStats` trait allow both source and sink statistics to share the same generic scraper infrastructure.
