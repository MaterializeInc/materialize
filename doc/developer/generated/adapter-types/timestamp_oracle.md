---
source: src/adapter-types/src/timestamp_oracle.rs
revision: 5d439bc71e
---

# mz-adapter-types::timestamp_oracle

Provides default constant values for the PostgreSQL-backed timestamp oracle connection pool configuration: max size (50), max wait (60 s), TTL (300 s), TTL stagger (6 s), connect timeout (5 s), TCP user timeout (30 s), and TCP keepalive idle/interval/retries.
These constants are referenced by the timestamp oracle implementation when constructing its pool's `DynamicConfig`.
