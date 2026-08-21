---
source: src/adapter/src/frontend_read_then_write.rs
revision: 00299a05e3
---

# adapter::frontend_read_then_write

Implements `INSERT [...] SELECT`, `DELETE`, and `UPDATE` using a subscribe with optimistic concurrency control (OCC), sequenced from the session task rather than the coordinator's main loop.
`PeekClient::frontend_read_then_write` is the main entry point. It validates the plan, acquires an OCC semaphore permit and a read hold, optimizes the selection into a subscribe dataflow, runs the OCC loop, and then handles the outcome by staging rows, submitting a blind write, or advancing the session's write timestamp.

The OCC semaphore serializes concurrent read-then-write operations process-wide, bounding the number that hold read holds simultaneously. The permit is acquired before the read hold so that queued operations do not pin compaction while waiting.

## Whether the write reads persisted state

Two predicates determine this. `validate_selection_dependencies` inspects `depends_on()` syntactically before the dataflow runs, because a read-dependent write inside a transaction must be refused while refusal is still possible. The subscribe answers dynamically at runtime: a channel that closes on its own means the selection's output frontier reached the empty antichain and the diffs are frontier-independent.

The syntactic answer decides transaction membership. A write that reads nothing stages its rows as `TransactionOps::Writes` and commits atomically with the rest of the transaction. A write that reads persisted state is refused inside an explicit transaction; in an extended-protocol pipeline it ends its own implicit transaction rather than spanning the rest of the pipeline, matching PostgreSQL's treatment of statements that cannot run in a transaction block.

The dynamic answer decides only how the write is submitted: a timestamped write from inside the OCC loop, or a blind submission after it. `end_own_transaction` clears staged ops for statements that committed on their own, preventing `TransactionStatus::may_span_pipeline` from holding the pipeline open.

Disagreement between the two predicates is caught by `soft_assert_or_log!` in the `Committed` and `NoRowsMatched` arms of the outcome handler.

## The OCC loop (`run_occ_loop`)

The loop drains the subscribe, accumulates diffs in `OccState::all_diffs`, and consolidates incrementally on each progress message. When the consolidated state is ready to write (progress past `as_of` and nonempty diffs), it submits a `Command::AttemptWrite` at the observed frontier as the write timestamp.

On `WriteResult::TimestampPassed` (concurrent write advanced the target's upper), the loop clears `write_submitted`, increments `retry_count`, and waits for the subscribe to advance before retrying. Retries are bounded by `max_occ_retries`; exceeding the limit yields `AdapterError::ReadThenWriteContention`.

A subscribe channel that closes on its own exits via `OccOutcome::Blind`, returning frontier-independent diffs for the caller to stage or submit. A consolidated empty result at a progress timestamp past `as_of` exits via `OccOutcome::NoRowsMatched`.

`process_message` handles the subscribe output format (`mz_timestamp`, `mz_progressed`, `mz_diff`, data columns), validates constraints on inserted rows via `RelationDesc::constraints_met`, and enforces `max_result_size`. Format mismatches are surfaced as `AdapterError::Internal` rather than a panic.

## Cancellation and write safety (`FrontendWriteAttemptState`)

`FrontendWriteAttemptState` coordinates between the attempt and its cancellation wrapper. `write_submitted` is set just before `Command::AttemptWrite` is sent and cleared only on `TimestampPassed` (the only outcome where the write definitively did not land). Cancellation and statement timeout check `write_submitted` and, if set, await the definitive write result rather than synthesizing an error, because the write may already be durable.

## Read linearization

`ensure_read_linearized` blocks until the governing timeline's oracle has advanced to `as_of`, implementing the strict-serializable read guarantee. It uses `governing_timeline` (not `TimelineContext::timeline()`) so that a timestamp-independent selection still linearizes against the `EpochMilliseconds` oracle. A single group-commit nudge is issued at the start of the wait to avoid polling for a full `default_timestamp_interval`. Linearization runs before the subscribe is created and, for `NoRowsMatched` with an observed timestamp, after the OCC loop, with the OCC permit released before the second wait.

## Key helpers

- `validate_read_then_write`: enforces the `mz_now()` ban, the dependency cap, timeline compatibility (EpochMilliseconds only), cluster liveness, and replica pin; returns a `ValidationResult` carrying cluster, replica, timeline, and table descriptor.
- `optimize_mir_read_then_write`: applies the mutation to the MIR selection via `apply_mutation_to_mir`, prepares unmaterializable functions one-shot, and runs the subscribe optimizer to produce a `GlobalMirPlan`.
- `apply_mutation_to_mir`: DELETE negates the expression; UPDATE wraps it in a `Let` binding and unions negated old rows with mapped new rows; INSERT passes through unchanged.
- `build_success_response`: builds the `ExecuteResponse` before writing, enforcing result-size caps row-by-row for RETURNING expressions to bound temporary allocation.
- `submit_blind_write`: sends `Command::AttemptWrite` with `write_ts: None`, letting group commit choose the timestamp.
- `classify_write_result`: maps `WriteResult` to `WriteOutcome`, translating `TargetChanged` to `ConcurrentDependencyMutation` for consistent client-visible error codes.
