# QA findings: incremental OCC read-then-write

Adversarial QA pass over the `adapter-read-then-write-occ-incremental` branch
(frontend OCC sequencing for DELETE / UPDATE / INSERT...SELECT, gated by
`enable_adapter_frontend_occ_read_then_write`).

Tests live in `src/environmentd/tests/server.rs`, prefixed `qa_occ_` /
`qa_legacy_`. Run them with the OCC binary already built:

```
METADATA_BACKEND_URL=postgres://root@localhost:26257/materialize \
  cargo nextest run -p mz-environmentd -E 'test(/qa_occ_/)'
# bug repros are #[ignore]d; reproduce with:
METADATA_BACKEND_URL=... cargo nextest run -p mz-environmentd --run-ignored only \
  -E 'test(qa_occ_far_future_refresh_mv_respects_statement_timeout) | test(qa_occ_far_future_rtw_starves_permit_pool)'
```

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

## Bug found: `ensure_read_linearized` ignores `statement_timeout`

`frontend_read_then_write::ensure_read_linearized` loops `tokio::time::sleep`
until the oracle reaches `as_of`, with **no `statement_timeout` check**. When a
read-then-write's `as_of` is far in the future — e.g. the read depends on a
`REFRESH AT <far future>` materialized view — the session hangs indefinitely.
Only client cancellation / disconnect frees it; `statement_timeout` does not.

This directly contradicts the code comment at the call site, which claims:

> By waiting here, a pathological RTW hits `statement_timeout` and returns
> without ever touching the oracle.

The "without ever touching the oracle" half holds (the write is never
submitted, so the EpochMilliseconds oracle is not bumped — good). The "hits
`statement_timeout`" half does not: `statement_timeout` is only enforced inside
`run_occ_loop` (via `tokio::time::timeout` on `recv`), which is reached *after*
linearization.

Repro: `qa_occ_far_future_refresh_mv_respects_statement_timeout` (#[ignore]d).
With `statement_timeout = '3s'`, `INSERT INTO dst SELECT a FROM mv` does not
return within 45s.

### Severity / scope

- **Not a pure regression**: the legacy (lock-based) path also hangs on a
  far-future read past `statement_timeout`
  (`qa_legacy_far_future_refresh_mv_statement_timeout`). The indefinite hang is
  pre-existing.
- **OCC-specific amplification (the real concern)**: the hung op holds an OCC
  semaphore permit (`max_concurrent_occ_writes`, default 4) for its entire
  indefinite lifetime, plus read holds on the read dependencies (pinning
  compaction). A handful of such ops exhaust the global permit pool and wedge
  **all** read-then-writes process-wide — including ones on unrelated tables —
  and those victims also ignore `statement_timeout` because they block on
  permit acquisition *before* the OCC loop. The legacy path only blocks writes
  to the *target* table (per-table write lock), so OCC widens the blast radius
  from one table to the whole RTW pipeline. Repro:
  `qa_occ_far_future_rtw_starves_permit_pool` (#[ignore]d) — an unrelated
  `UPDATE` does not return within 25s while one far-future RTW holds the sole
  permit (pool sized to 1).

### Corollary / operational footgun

The same "block before the timeout is enforced" structure means
`max_concurrent_occ_writes = 0` (settable via `system_parameter_default`;
`ALTER SYSTEM SET` is correctly rejected) sizes the semaphore to zero permits
and bricks every read-then-write after restart, with no `statement_timeout`
relief. Consider rejecting `0` (require `>= 1`).

### Suggested fixes (any of)

- Enforce `statement_timeout` (and ideally cancellation is already handled) in
  `ensure_read_linearized` — e.g. wrap the wait in `tokio::time::timeout`, or
  bound the as_of wait and return `AdapterError::StatementTimeout`.
- Acquire the OCC permit (and read holds) *after* linearization, or release the
  permit while parked, so a parked far-future op cannot starve the pool.
- Cap how far into the future an RTW `as_of` may be before erroring fast.
