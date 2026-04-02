---
source: src/environmentd/src/http/mcp.rs
revision: 2c1cdc28b4
---

# environmentd::http::mcp

Implements Model Context Protocol (MCP) HTTP handlers that expose Materialize data products to AI agents via JSON-RPC 2.0.
Provides two endpoints: `/api/mcp/agents` (tools: `get_data_products`, `get_data_product_details`, `read_data_product`, `query`) for user data products discovered via `mz_internal.mz_mcp_data_products`, and `/api/mcp/observatory` (tool: `query_system_catalog`) for read-only access to system catalog tables in schemas from `SYSTEM_SCHEMAS` (excluding `mz_unsafe`).
Each endpoint is gated by a dynamic feature flag (`ENABLE_MCP_AGENTS`, `ENABLE_MCP_OBSERVATORY`, `ENABLE_MCP_AGENTS_QUERY_TOOL`); response size is bounded by `MCP_MAX_RESPONSE_SIZE`.
Implements MCP protocol version `2025-11-25`: the `initialize` response includes a `protocolVersion` field set to `MCP_PROTOCOL_VERSION`, tool definitions carry `title`, `annotations` (`ToolAnnotations` with `readOnlyHint`, `destructiveHint`, `idempotentHint`, `openWorldHint`), and `ToolContentResult` includes an `isError` field.
Both endpoints accept GET requests with a 405 Method Not Allowed response (via `handle_mcp_method_not_allowed`) and validate the `Origin` header against the `Host` header to prevent DNS rebinding attacks, rejecting mismatches with 403 Forbidden.
Enforces read-only SQL validation and AST-based system-table access control before executing queries; the observatory endpoint allows SHOW and EXPLAIN statements without table references but rejects constant SELECT queries (e.g., `SELECT 1`) to prevent misuse for arbitrary computation.
`McpRequestError` maps domain errors to standard JSON-RPC error codes.
