This test suite checks the resumption logic for Postgres sources
by performing inserts and deletes on the Postgres side, injecting
some form of failure and then making sure that the Materialize
side has been able to resume replicating and fully catches up
after the interruption has been cleared.

The two different phases of Postgres replication are checked:
- interruptions during the initial snapshot
- interruptions during the actual replication

The following failures are injected:
- disconnecting Postgres from Materialize via toxiproxy
- restart of the Postgres server
- restart of the Materialize instance

This test suite additionally checks the failure logic for Postgres
sources by verifying that a source is put into a failed state
upon detecting a restore to a point-in-time backup of the Postgres
server

To run:

```bash
./mzcompose down -v ; ./mzcompose run default
```
