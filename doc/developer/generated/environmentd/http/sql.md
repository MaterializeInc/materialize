---
source: src/environmentd/src/http/sql.rs
revision: 7bc360dc8b
---

# environmentd::http::sql

Implements HTTP and WebSocket SQL execution for `environmentd`.
Provides `execute_request` (the core SQL dispatch loop used by both REST and MCP handlers), WebSocket upgrade handlers for streaming query results and SUBSCRIBE, and the `SqlResponse`/`SqlResult` types that serialize query output as JSON.
Handles session management, transaction lifecycle, COPY, and statement logging within the HTTP execution path. In `execute_promsql_query`, non-value label columns that are SQL NULL fall back to an empty string rather than panicking.
