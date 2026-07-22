---
source: src/sql/src/plan/statement/tcl.rs
revision: 75cdbb9e3f
---

# mz-sql::plan::statement::tcl

Plans transaction-control language statements: `BEGIN`/`START TRANSACTION`, `COMMIT`, `ROLLBACK`, and `SET TRANSACTION`.
Each produces the corresponding `Plan` variant (`StartTransactionPlan`, `CommitTransactionPlan`, `AbortTransactionPlan`, `SetTransactionPlan`).
`plan_set_transaction` rejects access modes (`READ ONLY`, `READ WRITE`) at plan time via `bail_unsupported!`, before producing any plan. This prevents partially-applied state (e.g. a silently changed isolation level) when a `SET TRANSACTION ... READ WRITE` statement is later rejected by the sequencer. The check mirrors `plan_start_transaction`, which validates modes at plan time.
