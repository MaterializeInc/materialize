# mysql-source-gtid-monotonicity-violation — MySQL Source Must Not Enter Errored State Due to Out-of-Order GTIDs

## Summary

The Materialize MySQL CDC source must never receive GTIDs out of monotonic
order from the multithreaded replica. If it does, `BinlogGtidMonotonicityViolation`
(a `DefiniteError`) permanently errors the source — there is no self-recovery path.

The pipeline is:

```
MySQL primary (GTID + WRITESET dependency tracking)
    |
    +--> MySQL replica (4 parallel workers, replica_preserve_commit_order=ON)
                |
         Materialize CDC source (mysql_cdc_source, antithesis_cluster)
                |
         antithesis_cdc table
```

With `replica_preserve_commit_order=ON` the replica guarantees it applies
transactions in primary-commit order even with 4 concurrent applier threads.
Under Antithesis fault injection — scheduling jitter, container kills at
arbitrary points, network delays — this guarantee is stress-tested.

## The Error

`DefiniteError::BinlogGtidMonotonicityViolation` is raised in
`src/storage/src/source/mysql/replication/partitions.rs:advance_frontier`
when the per-UUID GTID `active_part.timestamp() > new_part.timestamp()`:
a new GTID has a lower transaction-id than one the source already processed.

Error message: `"received out of order gtids for source {uuid} at transaction-id {txn}"`

Once emitted, this `DefiniteError` flows to `DataflowError::SourceError` and
the source is permanently in the "errored" state.  The only recovery is a
user-initiated `DROP SOURCE` + recreate.

## Instrumentation

**SUT-side** — `src/storage/src/source/mysql/replication/partitions.rs`.

`assert_unreachable!("mysql: BinlogGtidMonotonicityViolation — received out-of-order GTID from multithreaded replica", …)` fires immediately before the `DefiniteError` is returned.  This gives Antithesis a precise, reproducible anchor at the exact site where the violation is detected — before the error propagates and the source enters the errored state.

**Workload-side** — `test/antithesis/workload/test/anytime_mysql_source_no_gtid_errors.py`.

`anytime_` driver polls `mz_internal.mz_source_statuses` every 2 s for the
`mysql_cdc_source`.  When `status = 'errored'` AND `error` contains
`"out of order gtids"`, fires:

```python
always(
    not is_gtid_error,  # True normally; False triggers the property failure
    "mysql: source must not enter errored state due to out-of-order GTIDs",
    {"source": SOURCE_NAME, "status": status, "error": error, …},
)
```

The workload-side check is complementary: it observes the effect at the
user-visible surface, while the SUT-side assertion fires at the exact causal
site inside the source operator.

## Why This Property Matters

With `replica_preserve_commit_order=ON` enabled, out-of-order GTIDs should
be impossible.  This property tests whether Antithesis can find a schedule
(crash timing, worker scheduling delay, partial replica state) under which
the commit-order guarantee breaks down.  A violation surfaces as:

1. The SUT-side `assert_unreachable!` firing (gives Antithesis a replay anchor).
2. The source permanently stuck in "errored" state.
3. The `mysql-source-no-data-loss` `always()` assertions becoming vacuous
   (catchup never completes, liveness anchor never fires).

## Assertion Types Chosen

- `Unreachable` (SUT-side): the violation path in `advance_frontier` should
  never be reached.  `assert_unreachable!` converts the error site into a
  reportable Antithesis property.

- `always(not is_gtid_error)` (workload-side): the observable effect (source
  in "errored" state due to this error) must never be true.  `always()` is
  correct because this is a hard safety invariant — every observation must hold.

## Related Properties

- `mysql-source-no-data-loss` — shares the MySQL CDC pipeline; a GTID
  ordering violation will also cause the data-loss property assertions to
  become vacuous (catchup never completes once the source is errored).
- `storage-command-replay-idempotent` — MySQL CDC resume on clusterd restart
  also exercises GTID position tracking; a corrupted GTID state after restart
  could trigger this violation.

## Schema

```sql
-- MySQL: commit-order-preserving multithreaded replication
SET GLOBAL replica_parallel_workers = 4;
SET GLOBAL replica_preserve_commit_order = ON;

-- Materialize CDC source (reads from mysql-replica)
CREATE SOURCE mysql_cdc_source IN CLUSTER antithesis_cluster
    FROM MYSQL CONNECTION antithesis_mysql_conn;
CREATE TABLE antithesis_cdc
    FROM SOURCE mysql_cdc_source (REFERENCE antithesis.cdc_test);
```

## SUT Code Path

```
mysql/replication/partitions.rs :: GtidReplicationPartitions::advance_frontier
  -> active_part.timestamp() > new_part.timestamp()
  -> assert_unreachable!("mysql: BinlogGtidMonotonicityViolation …")  ← NEW
  -> DefiniteError::BinlogGtidMonotonicityViolation(source_id, txn_id)
  -> ReplicationError::Definite(…)
  -> source enters "errored" state permanently
```
