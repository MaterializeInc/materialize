---
source: src/adapter/src/session.rs
revision: 0a20c581ea
---

# adapter::session

Defines `Session`, the per-connection state object held by the coordinator for the duration of a client connection.
A `Session` tracks the connection ID and UUID, the current transaction state (`TransactionStatus`), open cursors, prepared statements with their logging metadata, portal bindings, session variables, role metadata, and lifecycle timestamps used for statement logging.
`SessionConfig` includes an `authenticator_kind: AuthenticatorKind` field that records which authenticator was used for the connection, and a `groups: Option<Vec<String>>` field that carries JWT group claims from OIDC authentication for group-to-role sync; non-OIDC paths set this to `None`.
`Transaction` holds the current transaction's operations (`TransactionOps`), a plan context, and optional write-lock guards; `TransactionStatus` is the state machine covering `Default`, `Started`, `InTransaction`, `InTransactionImplicit`, and `Failed` states.
The module also defines `RowBatchStream` (the channel type for streaming subscribe results to pgwire) and `PreparedStatement` (a parsed, described statement with optional logging info).
