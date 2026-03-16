---
source: src/adapter/src/session.rs
revision: 892cf626bc
---

# adapter::session

Defines `Session<T>`, the per-connection state object held by the coordinator for the duration of a client connection.
A `Session` tracks the connection ID and UUID, the current transaction state (`TransactionStatus`), open cursors, prepared statements with their logging metadata, portal bindings, session variables, role metadata, and lifecycle timestamps used for statement logging.
`Transaction` holds the current transaction's operations (`TransactionOps`), timestamp context, and read holds; `TransactionStatus` is the state machine covering `Default`, `Started`, `InTransaction`, `InTransactionImplicit`, and `Failed` states.
The module also defines `RowBatchStream` (the channel type for streaming subscribe results to pgwire) and `PreparedStatement` (a parsed, described statement with optional logging info).
