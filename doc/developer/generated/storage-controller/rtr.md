---
source: src/storage-controller/src/rtr.rs
revision: 00cc513fa5
---

# storage-controller::rtr

Implements real-time recency (RTR) timestamp resolution for external sources.
`real_time_recency_ts` fetches the current write frontier from an external system (Kafka, Postgres, MySQL, or SQL Server) and then reads the source's remap shard until it has ingested at least that frontier, returning the minimum Materialize timestamp at which the source reflects all externally visible data.
Load generator sources are explicitly unsupported and will panic if passed to this function.
