# QA findings: incremental OCC read-then-write

Adversarial QA pass over the `adapter-read-then-write-occ-incremental` branch
(frontend OCC sequencing for DELETE / UPDATE / INSERT...SELECT, gated by
`enable_adapter_frontend_occ_read_then_write`).

Tests live in `src/environmentd/tests/server.rs`, prefixed `qa_occ_` /
`qa_legacy_`. Run them with the OCC binary already built:

```
METADATA_BACKEND_URL=postgres://root@localhost:26257/materialize \
  cargo nextest run -p mz-environmentd -E 'test(/qa_occ_/)'
```

All `qa_occ_` tests — including the two former bug repros
(`qa_occ_far_future_refresh_mv_respects_statement_timeout` and
`qa_occ_far_future_rtw_starves_permit_pool`) — are now active and passing; they
verify the fix described below. The only `#[ignore]`d test is
`qa_legacy_far_future_refresh_mv_statement_timeout`, which documents the
pre-existing far-future hang on the legacy coordinator path (out of scope for
this OCC-only fix).

## What was verified correct (passing tests)

These exercise behaviors the design must preserve; all pass under OCC:

- `qa_occ_concurrent_delete_no_overdelete` — N concurrent `DELETE`s of a
  multiset row of multiplicity M sum to exactly M deleted, table ends empty,
  multiplicity never goes negative. OCC retry prevents over-delete.
- `qa_occ_duplicate_row_multiplicity_counts` — DELETE/UPDATE/INSERT...SELECT
  affected-row counts respect multiset multiplicity.
- `qa_occ_not_null_constraint_enforced` — `NOT NULL` violations via UPDATE and
  INSERT...SELECT error out and leave the table unchanged (no partial commit).
- `qa_occ_returning_values_correct` — INSERT RETURNING (constant and
  read-dependent INSERT...SELECT) returns the right rows/expressions. (Note:
  Materialize only parses `RETURNING` for INSERT, so the DELETE/UPDATE
  RETURNING branches in `build_success_response` are effectively dead code.)
- `qa_occ_update_moves_overlapping_rows` — row-moving UPDATEs
  (`SET id = id + 1`) produce the correct final set through the
  Let/Negate/map MIR transform and consolidation.
- `qa_occ_insert_select_from_mv` — INSERT...SELECT reading a materialized view
  (TimestampDependent timeline; linearization defaults to EpochMilliseconds).
- `qa_occ_concurrent_mixed_dml_no_internal_error` — concurrent
  UPDATE/DELETE/INSERT mix produces no internal errors / coordinator panics.

The core OCC retry/consolidation logic, the timestamped-write/oracle
interaction, and the snapshot/progress `NoRowsMatched` reasoning were also
reviewed statically and found sound.

## Fixed: OCC read-then-write now honors `statement_timeout` everywhere

**Original bug.** `frontend_read_then_write::ensure_read_linearized` loops
`tokio::time::sleep` until the oracle reaches `as_of`, with no
`statement_timeout` check. When a read-then-write's `as_of` was far in the
future — e.g. the read depends on a `REFRESH AT <far future>` materialized
view — the session hung indefinitely; only client cancellation / disconnect
freed it. `statement_timeout` was only enforced inside `run_occ_loop` (via
`tokio::time::timeout` on `recv`), which is reached *after* linearization, so
the pre-loop phases (planning, OCC permit acquisition, timestamp determination,
linearization) were all unbounded. This contradicted the call-site comment,
which claimed a pathological RTW "hits `statement_timeout` and returns".

The OCC-specific amplification was the real concern: the parked op holds an OCC
semaphore permit (`max_concurrent_occ_writes`, default 4) for its entire
lifetime. A handful of such ops exhausted the global permit pool and wedged
**all** read-then-writes process-wide — including ones on unrelated tables —
because those victims block on permit acquisition *before* the OCC loop, so
they too ignored `statement_timeout`. The legacy path only blocks writes to the
*target* table (per-table write lock), so OCC widened the blast radius from one
table to the whole RTW pipeline.

**The fix.** `statement_timeout` is now enforced **centrally**, in the
`tokio::select!` of `SessionClient::try_frontend_read_then_write_with_cancel`
(`src/adapter/src/client.rs`) — the one place that already owns the whole
operation's lifetime and handles cancellation. A new select arm sleeps for
`*session.vars().statement_timeout()` (treating `Duration::ZERO` as "off /
wait forever" via `futures::future::pending`, mirroring `run_occ_loop`'s
`effective_timeout`). When it fires it returns `AdapterError::StatementTimeout`
and forwards `Command::PrivilegedCancelRequest` to the coordinator (mirroring
the existing `cancel_future` arm) to clean up any in-flight coordinator-owned
work. Because the whole `try_frontend_read_then_write` future is dropped, the
OCC permit, read holds, and `SubscribeHandle` (whose `Drop` sends
`DropInternalSubscribe`) are all released.

This single deadline now bounds *every* phase: planning, OCC permit
acquisition, timestamp determination, `ensure_read_linearized`, **and** the OCC
loop. The far-future op times out promptly and releases its permit, so victims
behind a starved pool also honor their own `statement_timeout`. The in-loop
`run_occ_loop` timeout is kept as defense-in-depth (it also bounds a single
blocking `recv`); the call-site comment in `frontend_read_then_write.rs` was
corrected to point at the central enforcement.

Verified by `qa_occ_far_future_refresh_mv_respects_statement_timeout` (the
far-future op returns a timeout error well within budget) and
`qa_occ_far_future_rtw_starves_permit_pool` (the unrelated victim times out on
permit acquisition within its own `statement_timeout` instead of hanging).
Both are now active (no longer `#[ignore]`d) and passing.

### Out of scope / unchanged

- **Legacy path.** The legacy (lock-based) coordinator path also hangs on a
  far-future read past `statement_timeout`
  (`qa_legacy_far_future_refresh_mv_statement_timeout`, still `#[ignore]`d).
  This pre-existing hang is intentionally left unchanged; the fix is OCC-only.

### Corollary / operational footgun (still open)

The same "block before the timeout is enforced" structure used to mean
`max_concurrent_occ_writes = 0` (settable via `system_parameter_default`;
`ALTER SYSTEM SET` is correctly rejected) sizes the semaphore to zero permits
and bricks every read-then-write after restart. With the central timeout, such
operations now at least *time out* instead of hanging forever, but a value of
`0` is still a footgun — consider rejecting it (require `>= 1`).
