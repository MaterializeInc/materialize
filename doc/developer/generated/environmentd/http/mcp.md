---
source: src/environmentd/src/http/mcp.rs
revision: df9e016020
---

# environmentd::http::mcp

Implements Model Context Protocol (MCP) HTTP handlers that expose Materialize data products to AI agents via JSON-RPC 2.0.
Provides two endpoints: `/api/mcp/agent` (tools: `get_data_products`, `get_data_product_details`, `read_data_product`, `query`) for user data products discovered via `mz_internal.mz_mcp_data_products`, and `/api/mcp/developer` (tool: `query_system_catalog`) for read-only access to system catalog tables in schemas from `SYSTEM_SCHEMAS` (excluding `mz_unsafe`).
Each endpoint is gated by a dynamic feature flag (`ENABLE_MCP_AGENT`, `ENABLE_MCP_DEVELOPER`, `ENABLE_MCP_AGENT_QUERY_TOOL`); response size is bounded by `MCP_MAX_RESPONSE_SIZE`.
Each request tags the session's `application_name` with `mz_mcp_agents` or `mz_mcp_developer` (via `set_default`, so a caller-supplied value still takes precedence) to make MCP-originated sessions visible in `mz_session_history` and `mz_statement_execution_history`.
Implements MCP protocol version `2025-11-25`: the `initialize` response includes a `protocolVersion` field set to `MCP_PROTOCOL_VERSION`, tool definitions carry `title`, `annotations` (`ToolAnnotations` with `readOnlyHint`, `destructiveHint`, `idempotentHint`, `openWorldHint`), and `ToolContentResult` includes an `isError` field.
Both endpoints' `initialize` responses include static usage instructions (via `endpoint_instructions`). The developer endpoint guides AI agents to use `mz_internal.mz_ontology_*` tables for discovering table schemas, join paths, and column names before writing queries. The agent endpoint instructs agents to prefer indexed objects (served from memory) over unindexed materialized views, and to pass the `cluster` parameter to `read_data_product` when the data product's cluster differs from the session cluster.
Both endpoints accept GET requests with a 405 Method Not Allowed response (via `handle_mcp_method_not_allowed`) and validate the `Origin` header against the CORS allowlist (injected as `Arc<Vec<HeaderValue>>` via Axum `Extension`) to prevent DNS rebinding attacks, rejecting origins not on the allowlist with 403 Forbidden. The CORS layer alone is insufficient because DNS rebinding causes the browser to treat the request as same-origin, bypassing preflight; the server-side check via `mz_http_util::origin_is_allowed` closes this gap.
Enforces read-only SQL validation and AST-based system-table access control before executing queries; the developer endpoint allows SHOW and EXPLAIN statements without table references but rejects constant SELECT queries (e.g., `SELECT 1`) to prevent misuse for arbitrary computation.
`McpRequestError` maps domain errors to standard JSON-RPC error codes.
