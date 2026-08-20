---
source: src/environmentd/src/http/sql.rs
revision: b0d4c751f6
---

# environmentd::http::sql

Implements HTTP and WebSocket SQL execution for `environmentd`.
Provides `execute_request` (the core SQL dispatch loop used by both REST and MCP handlers), WebSocket upgrade handlers for streaming query results and SUBSCRIBE, and the `SqlResponse`/`SqlResult` types that serialize query output as JSON.
`SqlResult` has two shapes: the buffered `Rows` variant (used by the JSON REST transport, holds pre-serialized `Vec<Box<serde_json::value::RawValue>>` rows to avoid the memory overhead of a `serde_json::Value` tree) and the streaming `StatementResult::Rows` variant (used by the WebSocket transport, carries a `RecordFirstRowStream` consumed lazily so large results are not buffered whole).
Handles session management, transaction lifecycle, COPY, and statement logging within the HTTP execution path. When `enable_statement_arrival_logging` is on, `execute_request` logs arriving statements at info level before processing them, with SQL literals redacted and bind parameter values replaced by their count. In `execute_promsql_query`, non-value label columns that are SQL NULL fall back to an empty string rather than panicking.
