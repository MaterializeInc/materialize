# QA findings: incremental OCC read-then-write

Findings from an adversarial QA pass over frontend OCC sequencing for DELETE,
UPDATE and INSERT ... SELECT, gated by
`enable_adapter_frontend_occ_read_then_write`. The properties recorded here are
pinned by the `qa_occ_`-prefixed tests in `src/environmentd/tests/server.rs`:

```
METADATA_BACKEND_URL=postgres://root@localhost:26257/materialize \
  cargo nextest run -p mz-environmentd -E 'test(/qa_occ_/)'
```

## `statement_timeout` bounds the whole operation, not just the OCC loop

Enforcement sits in the `tokio::select!` of
`SessionClient::try_frontend_read_then_write_with_cancel`
(`src/adapter/src/client.rs`). That frame owns the operation's entire lifetime
and already handles cancellation, so a deadline placed there covers every phase:
planning, OCC permit acquisition, timestamp determination, read linearization,
and the retry loop. A `statement_timeout` of zero means "no deadline" and is
represented by `futures::future::pending`.

A deadline placed further in would leave phases unbounded, and the unbounded
phases are the dangerous ones. `ensure_read_linearized` sleeps until the oracle
reaches the read's `as_of`, so a read whose `as_of` lies far in the future, for
example one depending on a `REFRESH AT '3000-01-01'` materialized view, parks
there for years. Permit acquisition happens before the loop is entered at all,
so a victim of such a parked operation would never reach an in-loop deadline
either.

When the deadline fires, the statement reports `AdapterError::StatementTimeout`
and forwards `Command::PrivilegedCancelRequest` to the coordinator to clean up
coordinator-owned work, mirroring the cancellation arm beside it. Dropping the
`try_frontend_read_then_write` future releases the OCC permit, the read holds,
and the `SubscribeHandle`, whose `Drop` sends `DropInternalSubscribe`.

## Permit starvation has a wider blast radius than a per-table write lock

A parked read-then-write holds its OCC semaphore permit
(`max_concurrent_occ_writes`, default 4) for its whole lifetime. A handful of
parked operations exhaust the pool and stall every read-then-write in the
process, including ones on unrelated tables, because a waiter blocks on permit
acquisition before doing anything else. The lock-based coordinator path cannot
do that: it takes a write lock on the target table, so it only blocks writes to
that table.

That asymmetry is why the deadline above has to cover the permit wait. It bounds
the victims of a starved pool, not just the operation that starves it.

`statement_timeout = 0` removes that bound, so the starvation becomes permanent:
four sessions with no deadline, each reading a far-future `REFRESH` materialized
view, park in `ensure_read_linearized` holding the whole pool, and every
read-then-write in the process fails or hangs until one of them is cancelled.

Moving permit acquisition after linearization would fix that case, and we do not
do it, because the permit is deliberately acquired before the read holds. A
waiter that queued while holding read holds would pin compaction on its read
dependencies for as long as it waits, which is what happens under ordinary write
contention rather than only in the far-future case. So the ordering trades a
rare unbounded case against a common bounded one. Sequencing it as read holds,
then linearize, then permit would swap those, not remove the trade.

## `max_concurrent_occ_writes` must be at least 1

A value of 0 sizes the semaphore to zero permits, so every read-then-write in
the process waits out its `statement_timeout` and then fails. The parameter
carries a domain constraint requiring at least 1, which covers both ways of
setting it, `ALTER SYSTEM SET` and `system_parameter_default`.

`ALTER SYSTEM SET`/`RESET` of the parameter is accepted, because the value is
sampled once at boot and the running process cannot observe a later change. The
statement warns that the change only takes effect when `environmentd` restarts.

## The coordinator path blocks on a far-future read until its own timeout

`INSERT INTO dst SELECT a FROM mv`, where `mv` is a `REFRESH AT '3000-01-01'`
materialized view, blocks on the lock-based coordinator path while holding the
target table's write lock. It does not block forever. That path arms
`statement_timeout` around the row stream it reads the selection from, in
`sequencer::inner`, with zero mapped to `Duration::MAX`, so the statement fails
after the deadline and only `statement_timeout = 0` makes the block permanent.

The blast radius is the target table rather than the whole process, per the
argument above, which is the one respect in which the coordinator path is better
behaved here.

This was first recorded as an unconditional hang. If you see one with a non-zero
`statement_timeout`, the cause is not a missing deadline and the note above is
where to stop looking. The check is cheap: with the OCC flag off,
`SET statement_timeout = '5s'` and then the INSERT above should fail in about
five seconds.

## `RETURNING` is only parsed for `INSERT`

The parser rejects `RETURNING` on DELETE and UPDATE, so the DELETE and UPDATE
arms of the RETURNING handling in `build_success_response` are unreachable. They
exist because that code dispatches on `MutationKind`, but no test can exercise
them.

## Reviewed without findings

The OCC retry and consolidation logic, the interaction between timestamped
writes and the oracle, and the reasoning that distinguishes an empty snapshot
from the initial subscribe progress were reviewed against the code and produced
no finding.
