---
source: src/adapter/src/catalog/timeline.rs
revision: a632912d24
---

# adapter::catalog::timeline

Provides `CatalogState` methods for resolving the timeline context of catalog items: `get_timeline_context` returns the `TimelineContext` (user timeline, EpochMilliseconds, or timestamp-independent) for a collection, and `timedomain_for` computes the set of collections that share a timeline with a given query.
These are used by timestamp selection to determine which timestamp oracle to consult and to validate that a query does not mix incompatible timelines.
