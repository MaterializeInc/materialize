---
source: src/adapter/src/coord/sequencer/inner.rs
revision: a29f0a64ed
---

# adapter::coord::sequencer::inner

Implements the majority of `sequence_*` methods: general DDL (CREATE/DROP/ALTER for sources, sinks, connections, tables, types, roles, schemas, databases, network policies), DML (INSERT, UPDATE, DELETE via `sequence_read_then_write`), transaction management, cursor operations, COPY FROM, SHOW, SET/RESET, FETCH, RAISE, and more.
Each method validates the plan against the current catalog and session state, performs the catalog transaction, applies catalog implications, and returns the appropriate `ExecuteResponse`.
The `inner` sub-modules split out the most complex individual statement types (peek, subscribe, cluster, index, materialized view, view, continual task, copy_from, secret, explain_timestamp) into separate files.
