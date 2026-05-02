---
source: src/environmentd/src/http/mcp.rs
revision: ad7778e7b2
---

# environmentd::http::mcp

Implements Model Context Protocol (MCP) HTTP handlers that expose Materialize data products to AI agents via JSON-RPC 2.0.
Provides two endpoints: `/api/mcp/agent` (tools: `get_data_products`, `get_data_product_details`, `read_data_product`, `query`) for user data products discovered via `mz_internal.mz_mcp_data_products`, and `/api/mcp/developer` (tool: `query_system_catalog`) for read-only access to system catalog tables in schemas from `SYSTEM_SCHEMAS` (excluding `mz_unsafe`).
Each endpoint is gated by a dynamic feature flag (`ENABLE_MCP_AGENT`, `ENABLE_MCP_DEVELOPER`, `ENABLE_MCP_AGENT_QUERY_TOOL`); response size is bounded by `MCP_MAX_RESPONSE_SIZE`.
Implements MCP protocol version `2025-11-25`: the `initialize` response includes a `protocolVersion` field set to `MCP_PROTOCOL_VERSION`, tool definitions carry `title`, `annotations` (`ToolAnnotations` with `readOnlyHint`, `destructiveHint`, `idempotentHint`, `openWorldHint`), and `ToolContentResult` includes an `isError` field.
The developer endpoint's `initialize` response includes static usage instructions (via `endpoint_instructions`) that guide AI agents to use `mz_internal.mz_ontology_*` tables for discovering table schemas, join paths, and column names before writing queries. The agent endpoint returns no instructions.
Both endpoints accept GET requests with a 405 Method Not Allowed response (via `handle_mcp_method_not_allowed`) and validate the `Origin` header against the CORS allowlist (injected as `Arc<Vec<HeaderValue>>` via Axum `Extension`) to prevent DNS rebinding attacks, rejecting origins not on the allowlist with 403 Forbidden. The CORS layer alone is insufficient because DNS rebinding causes the browser to treat the request as same-origin, bypassing preflight; the server-side check via `mz_http_util::origin_is_allowed` closes this gap.
Enforces read-only SQL validation and AST-based system-table access control before executing queries; the developer endpoint allows SHOW and EXPLAIN statements without table references but rejects constant SELECT queries (e.g., `SELECT 1`) to prevent misuse for arbitrary computation.
`McpRequestError` maps domain errors to standard JSON-RPC error codes.
