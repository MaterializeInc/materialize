---
source: src/sql/src/plan/statement/tcl.rs
revision: c620d96cf4
---

# mz-sql::plan::statement::tcl

Plans transaction-control language statements: `BEGIN`/`START TRANSACTION`, `COMMIT`, `ROLLBACK`, and `SET TRANSACTION`.
Each produces the corresponding `Plan` variant (`StartTransactionPlan`, `CommitTransactionPlan`, `AbortTransactionPlan`, `SetTransactionPlan`).
