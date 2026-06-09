# mysql-source-no-data-loss — Every Row Written to MySQL Primary Is Eventually Visible in Materialize

## Summary

Every row inserted to the MySQL primary must eventually appear — with the
correct value — in the Materialize CDC source that reads from the
multithreaded MySQL replica. The pipeline is:

```
MySQL primary  --GTID binlog-->  MySQL replica (4 parallel workers)
                                      |
                              Materialize CDC source
                              (antithesis_cluster)
                                      |
                              antithesis_cdc table
```

## Instrumentation

**Workload-side** — `test/antithesis/workload/test/parallel_driver_mysql_cdc.py`.

Each `parallel_driver_` invocation:
1. Assigns a per-invocation `batch_id` prefix (Antithesis-seeded RNG).
2. Inserts `ROWS_PER_INVOCATION` (20) rows to `antithesis.cdc_test` on the
   MySQL primary, recording the expected `{id → value}` map locally.
3. Requests an Antithesis quiet period (25 s) and polls `antithesis_cdc` in
   Materialize until all expected rows appear or the 90 s budget expires.
4. Fires:
   - `sometimes("mysql: CDC source caught up to all primary inserts within catchup budget", …)`
     — liveness anchor; confirms at least one invocation reaches full catchup.
   - `always("mysql: CDC source row has correct value after catchup", …)` — safety;
     fired once per row, catches wrong-value corruption.
   - `always("mysql: CDC source row count matches inserted count after catchup", …)`
     — safety; catches extra phantom rows (count > expected) or missing rows
     (count < expected) at the batch level.

**First-run setup** — `test/antithesis/workload/test/first_mysql_replica_setup.py`.

Runs once per Antithesis timeline before any parallel drivers start:
- Creates `antithesis.cdc_test` on the primary.
- Configures the replica channel (`CHANGE REPLICATION SOURCE TO … SOURCE_AUTO_POSITION=1`).
- Sets `replica_parallel_workers = 4`, `replica_preserve_commit_order = ON`.
- Starts the replica.
- Creates the Materialize connection (`antithesis_mysql_conn`), source
  (`mysql_cdc_source`), and table (`antithesis_cdc`).
- Fires `reachable("mysql: first-run setup complete …")` so Antithesis can
  confirm the setup path is exercised in every timeline.
- Fires `sometimes("mysql replica: antithesis.cdc_test replicated from primary within 90s", …)`
  to confirm initial replication is flowing before the source is created.

## Why This Property Matters

MySQL CDC via a multithreaded replica is a distinct and failure-prone code
path compared to the Kafka/upsert path that the existing drivers exercise.
Key fault scenarios exposed:

- **Replica lag under faults** — if Antithesis kills the MySQL replica
  container, the replica restarts from its persisted GTID position (the
  replica data volume is persistent). The Materialize source must reconnect
  and resume without dropping rows.

- **Parallel replication ordering** — with 4 parallel workers and
  `replica_preserve_commit_order=ON`, the replica applies transactions
  concurrently but in primary commit order. Antithesis can inject scheduling
  jitter that stresses the ordering protocol.

- **Primary kills** — if Antithesis kills the MySQL primary, the replica
  loses its upstream. Materialize's CDC source must handle the replica going
  silent gracefully (not panic, not report wrong data).

- **Materialize clusterd restarts** — the MySQL CDC source resumes from the
  last committed GTID in the persist shard, similar to the Kafka source
  resume-offset logic. Existing `storage-command-replay-idempotent` property
  is stressed through the MySQL code path.

## Assertion Types Chosen

- `sometimes(…)` for liveness (catchup): the system must make progress at
  least once per run. Under heavy fault injection catchup may not complete
  every invocation; that's expected. We care that it succeeds at least once.

- `always(…)` for safety (per-row value, batch count): once we've confirmed
  catchup, every observable row must be correct. This is a hard safety
  invariant.

- `reachable(…)` for setup completion: ensures Antithesis counts the
  first-run setup as an exercised path across the run.

## Related Properties

- `storage-command-replay-idempotent` — MySQL CDC resume on clusterd restart
  exercises the same command-history replay path as Kafka sources.
- `fault-recovery-exercised` — the `sometimes(…)` recovery probe also fires
  after MySQL-induced coordinator failures.
- `kafka-source-survives-clusterd-restart` — shares the "source resumes after
  storage worker kill" structure; MySQL adds the replica-replication dimension.

## Schema

```sql
-- MySQL (primary and replica via replication):
CREATE TABLE antithesis.cdc_test (
    id         VARCHAR(64) NOT NULL PRIMARY KEY,
    batch_id   VARCHAR(64) NOT NULL,
    value      TEXT NOT NULL,
    updated_at TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6)
                ON UPDATE CURRENT_TIMESTAMP(6)
);

-- Materialize:
CREATE SECRET antithesis_mysql_password AS '…';
CREATE CONNECTION antithesis_mysql_conn TO MYSQL (
    HOST 'mysql-replica', USER 'root',
    PASSWORD SECRET antithesis_mysql_password
);
CREATE SOURCE mysql_cdc_source IN CLUSTER antithesis_cluster
    FROM MYSQL CONNECTION antithesis_mysql_conn;
CREATE TABLE antithesis_cdc
    FROM SOURCE mysql_cdc_source (REFERENCE antithesis.cdc_test);
```
