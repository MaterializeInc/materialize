# Architecture review — `sql::plan::statement`

Scope: `src/sql/src/plan/statement/` and `ddl/` subdirectory (≈ 13,917 LOC).

## 1. `ddl.rs` locality collapse: 30 object domains in one 8,137-line file

**Evidence**
- `src/sql/src/plan/statement/ddl.rs` — 110 `describe_*`/`plan_*` functions spanning:
  databases (line 216), schemas (236), tables (275), sources (756–1741),
  table-from-source (1742–2546), views (2547), MVs (2760), continual tasks
  (3124), sinks (3424–4295), indexes (4296), types (4468), roles (4619),
  clusters (4863–5488), secrets (5489), connections (5525), DROP (5602–5930),
  ALTER variants (5917–8137).
- The only extracted sub-domain is `ddl/connection.rs` (929 LOC). Every other
  domain is inline.

**Deletion test**
Split sources (756–1741 ≈ 832 LOC) and sinks (3424–4295 ≈ 872 LOC) into
`ddl/source.rs` and `ddl/sink.rs`. Would complexity vanish? Yes — both domains
import disjoint sets of storage-type imports; their connector-specific helpers
(`plan_kafka_source_connection`, `plan_postgres_source_connection`, etc.) have
no cross-references to the cluster or type planning functions. The split follows
the already-established `ddl/connection.rs` precedent. Complexity would
_vanish_ from `ddl.rs`, not relocate.

**Problem**
- **Locality:** a change to Kafka source planning (e.g. `plan_kafka_source_connection`)
  requires navigating an 8K-line file and searching past 3,000 lines of
  unrelated DDL to reach line 1196.
- **Scope creep:** `ddl.rs` owns connector-specific logic (RocksDB options,
  Avro schema, Kafka offset types) that semantically belongs to the source
  domain, not to generic DDL dispatch.

**Solution sketch**
Follow `ddl/connection.rs` precedent: extract `ddl/source.rs`, `ddl/sink.rs`,
`ddl/cluster.rs` (lines 4863–5488, ~626 LOC). Keep `ddl.rs` as a thin
re-exporting dispatcher that delegates to each sub-module. No behavioral change;
pure file boundary refactor.

**Risk**
Low — all functions are already isolated pairs. The `pub use ddl::…` re-exports
in `statement.rs` can forward through unchanged. Integration tests exercise the
planning output, not file structure.

## 2. (Honest skip) Parallel `describe_*`/`plan_*` pairs

Every DDL statement has both a `describe_*` and a `plan_*` function.
`describe_*` stubs are uniformly 3–5 lines; the asymmetry is intentional
(PostgreSQL wire protocol needs a description before execution). Deletion test
fails — collapsing them would entangle parameter-type inference with planning.
Not friction.
