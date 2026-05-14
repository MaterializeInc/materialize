# mysql-myisam-cdc-no-data-loss

## Summary

Every row inserted to a MyISAM table on the MySQL primary must eventually appear, with the correct value, in the Materialize CDC source that reads from the multithreaded replica — under the same guarantees as the InnoDB-backed `mysql-source-no-data-loss`.

This property exists separately from `mysql-source-no-data-loss` to cover the non-transactional DML axis: MyISAM in MySQL has fundamentally different transactional semantics, and Materialize's MySQL source code path doesn't distinguish engines, so we assert the engine-agnostic contract holds in practice.

## Why MyISAM is interesting for CDC testing

MyISAM differs from InnoDB in ways that show up in the binlog event stream:

* **No multi-statement transactions.** BEGIN/COMMIT around MyISAM statements is silently ignored. Every MyISAM statement is its own implicit transaction and gets its own GTID — there is no bundling of multiple statements under one GTID block.
* **No rollback.** A statement that fails partway through (e.g., a multi-row INSERT killed by a fault between rows 50 and 51) leaves the partial result committed. Whatever rows made it to the engine are durable; nothing rolls back.
* **Table-level locking instead of row-level.** Concurrent writers serialize rather than abort-and-retry. The binlog sees a strict serial order.
* **No crash recovery via redo log.** A crash mid-statement on the primary can leave the on-disk MyISAM table inconsistent with the binlog, but for our purposes we run against a healthy primary and read CDC from the replica, so this only matters under specific Antithesis fault-injection patterns.

Materialize's MySQL source decodes binlog events without consulting the upstream engine. ROW-format binlog events look identical for MyISAM and InnoDB. The source's expected contract is "every binlog event is reflected in the materialize table"; we assert that this contract holds when the upstream is MyISAM.

## Code paths

Same as `mysql-source-no-data-loss`:
- `src/storage/src/source/mysql/replication/partitions.rs` — binlog event decoding, GTID monotonicity check.
- `src/storage/src/source/mysql/snapshot.rs` — initial snapshot from the replica (uses `LOCK TABLES ... READ` for non-transactional engines).
- `src/mysql-util` — connection management.

No engine-specific code in the source. That's the property we're verifying.

## How to check it

Workload procedure (per invocation):
1. Pick a per-invocation `batch_id` prefix (`myi-p<u64hex>`) so concurrent drivers — including the InnoDB sibling — don't collide.
2. Insert 20 rows into `antithesis.cdc_test_myisam` on the MySQL primary. Each INSERT is its own implicit transaction.
3. Record the {id → value} map locally.
4. Request an Antithesis quiet period.
5. Poll `COUNT(*) FROM antithesis_cdc_myisam WHERE batch_id = ?` until it reaches the inserted count or the budget expires.
6. For each row, `SELECT value FROM antithesis_cdc_myisam WHERE id = ?` with `real_time_recency=true`. Assert `value` matches the locally-recorded one.

## What goes wrong on violation

Same failure modes as `mysql-source-no-data-loss`: rows missing, rows with wrong values, rows with extra entries. The bug is silent — the workload sees plausible-but-wrong data.

A MyISAM-specific failure mode worth flagging in triage: if the materialize source were to *accidentally* treat MyISAM events differently (e.g., conflate the lack-of-transaction with the lack-of-event), we'd see consistent under-counting on the MyISAM subsource while the InnoDB sibling looks healthy.

## Antithesis angle

The same fault classes that hit `mysql-source-no-data-loss` apply:
- Mysql primary container pause / restart between insert and binlog flush.
- Mysql-replica container pause / restart between binlog ingestion and materialize-side consumption.
- Materialized container pause / restart between CDC ingestion and persist append.
- Clusterd-pool container pause / restart on the cluster running the MySQL source.

Specifically MyISAM-relevant scenarios:
- A multi-row INSERT killed mid-statement should leave only the rows that actually committed. The replica's binlog reflects exactly those rows. Materialize must see exactly those rows. (The driver inserts row-by-row in a Python loop so we don't directly exercise "kill a multi-row INSERT," but Antithesis can pause the primary between any two row-INSERTs in the loop, achieving the same shape.)
- GTID ordering with MyISAM is per-statement: a workload that interleaves MyISAM and InnoDB writes produces an alternating GTID stream. Materialize must honor that ordering. (The InnoDB sibling driver and this driver run as independent parallel-workload invocations, naturally producing interleaved binlog events.)

## Dependencies

- Requires `gtid_mode = ON` and `binlog_format = ROW` on the primary (already set by mzcompose).
- Requires the MyISAM table on the primary AND on the replica (provisioned by `first_mysql_replica_setup.py`).
- The Materialize MySQL source must include the MyISAM table as a referenced subsource (`ensure_mysql_cdc_myisam_table()` in `helper_mysql_source.py`).

## Existing instrumentation

None engine-specific. The general `mysql-source-gtid-monotonicity-violation` SUT assertion (introduced 2026-05-14) covers GTID ordering for both engines uniformly.

## Implementation status

Implemented as `test/antithesis/workload/test/parallel_driver_mysql_myisam.py`.

| Message | Type | Fires when |
|---------|------|------------|
| `"mysql myisam: CDC source row has correct value after catchup"` | `always` | Per row, after catchup. False ⟺ row missing or value wrong. |
| `"mysql myisam: CDC source row count matches inserted count after catchup"` | `always` | Per invocation, after catchup. False ⟺ extra or missing rows for this batch. |
| `"mysql myisam: CDC source caught up to all primary inserts after quiet period"` | `sometimes` | Per invocation. Liveness for the catchup gate. |
| `"mysql replica: both cdc_test tables replicated from primary within 90s"` | `sometimes` | Per timeline (fires once from `first_mysql_replica_setup`). Confirms replication is flowing for both engines. |

Knobs: `ROWS_PER_INVOCATION=20`, `QUIET_PERIOD_S=25`, `CATCHUP_TIMEOUT_S=90.0`.

## Provenance

Surfaced by: Data Integrity (engine-agnostic CDC contract).
