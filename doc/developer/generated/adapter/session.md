---
source: src/adapter/src/session.rs
revision: fd5141bb8c
---

# adapter::session

Defines `Session`, the per-connection state object held by the coordinator for the duration of a client connection.
A `Session` tracks the connection ID and UUID, the current transaction state (`TransactionStatus`), open cursors, prepared statements with their logging metadata, portal bindings, session variables, role metadata, and lifecycle timestamps used for statement logging.
`SessionConfig` includes an `authenticator_kind: AuthenticatorKind` field that records which authenticator was used for the connection, and a `groups: Option<Vec<String>>` field that carries JWT group claims from OIDC authentication for group-to-role sync; non-OIDC paths set this to `None`.
`Transaction` holds the current transaction's operations (`TransactionOps`), a plan context, and optional write-lock guards; `TransactionStatus` is the state machine covering `Default`, `Started`, `InTransaction`, `InTransactionImplicit`, and `Failed` states.
`TransactionStatus::allows_writes` permits adding write operations only when the current ops are `TransactionOps::None` (and the transaction is not explicitly `ReadOnly`) or `TransactionOps::Peeks` whose timestamp context is constant (no timestamp assigned) and whose access mode is not `ReadOnly`. A read-only transaction is rejected even when all prior peeks were constant, preventing a constant-peek transaction from silently transitioning into a write transaction when the transaction was opened with `BEGIN READ ONLY` or an equivalent access-mode constraint.
The module also defines `RowBatchStream` (the channel type for streaming subscribe results to pgwire) and `PreparedStatement` (a parsed, described statement with optional logging info).
