# adapter::coord::sequencer

Top-level dispatch from `Plan` (the SQL compiler's output) to per-statement
sequencing logic on the `Coordinator`. Implements `Coordinator::sequence_plan`,
which matches each `Plan` variant and calls the appropriate `sequence_*`
method.

## Files

- `sequencer.rs` — top-level `sequence_plan` dispatch + shared utilities (`statistics_oracle`, `eval_copy_to_uri`, `check_log_reads`, `emit_optimizer_notices`, `return_if_err!` macro). Handles cross-cutting concerns: RBAC checks, transaction validity, COPY TO URI scheme validation (`s3://`, `gs://`).
- `inner.rs` *(this file is large; it's the bulk of the sequencer)* — implementations for the simpler statement classes: most DDL (CREATE/DROP/ALTER for sources, sinks, connections, tables, types, roles, schemas, databases, network policies), DML (INSERT/UPDATE/DELETE via `sequence_read_then_write`), transaction control, cursor ops, COPY FROM dispatch, SHOW, SET/RESET, FETCH, RAISE. Also defines the `Staged` / `StagedContext` / `StageResult` traits and the `sequence_staged` driver loop.
- `inner/` *(see [`inner/CONTEXT.md`](inner/CONTEXT.md))* — per-statement implementations for the most complex statement types (peek, subscribe, cluster, index, materialized view, view, continual task, copy_from, secret, explain_timestamp). Each implements `Staged` for its own stage enum.

## Key concepts

- **`Plan`** *(from `mz_sql::plan`)* — the SQL compiler's output: a typed enum where each variant is a planned statement.
- **`sequence_*` methods** — one per `Plan` variant; turn a `Plan` into catalog mutations + dataflow installs + `ExecuteResponse`.
- **`Staged` trait** — the multi-stage execution contract for statement types whose pipelines must yield to off-thread work (optimizer, timestamp linearization, real-time recency). Stages return either `Immediate` (loop again) or `Handle` (off-thread task; resume via `Coordinator::message_handler`).
- **`PlanValidity`** — captures the catalog/session preconditions a plan was compiled against; re-checked between stages so concurrent DDL invalidates an in-flight peek/subscribe rather than allowing it to produce stale results.

## Architecture notes

- The sequencer is the *seam* between SQL planning and execution. Above it is `mz_sql::plan` (pure compilation; no side effects); below it is `Coordinator` (catalog + controllers + dataflow installation).
- Statement types split between `inner.rs` (one file, many short methods) and `inner/` (multiple files, few long methods) by complexity rather than by category. The split is operational: `inner/` exists where a handler grew long enough to warrant its own file.
- The `Staged` trait is the codebase's main mechanism for cooperative multitasking on the coordinator's single-threaded loop. It appears nowhere outside this directory.

See `inner/ARCH_REVIEW.md` for friction patterns observed at the leaf level.

## Cross-references

- Caller: `Coordinator::handle_execute` in `coord/command_handler.rs` invokes `sequence_plan`.
- Compilation: `mz_sql::plan::Plan` and `mz_sql::plan::statement::*`.
- Generated developer docs: `doc/developer/generated/adapter/coord/sequencer.md` and `doc/developer/generated/adapter/coord/sequencer/`.
