---
source: src/adapter-types/src/timestamp_oracle.rs
revision: 6c2b81feaf
---

# mz-adapter-types::timestamp_oracle

Provides default constant values for the PostgreSQL-backed timestamp oracle connection pool configuration: max size (50), max wait (60 s), TTL (300 s), TTL stagger (6 s), connect timeout (5 s), TCP user timeout (30 s), TCP keepalive idle (10 s), keepalive interval (5 s), keepalive retries (5), and statement timeout (0 s, which is a sentinel meaning "do not set a statement timeout").
These constants are referenced by the timestamp oracle implementation when constructing its pool's `DynamicConfig`.
