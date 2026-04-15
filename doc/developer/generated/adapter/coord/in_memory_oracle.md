---
source: src/adapter/src/coord/in_memory_oracle.rs
revision: bce428d203
---

# adapter::coord::in_memory_oracle

Implements `InMemoryTimestampOracle`, a per-session in-memory timestamp oracle used for single-statement transactions and the serializable isolation level.
The oracle maintains a monotonically increasing write timestamp and uses it to assign read timestamps that satisfy causality constraints for the session, without consulting the durable CockroachDB-backed oracle.
