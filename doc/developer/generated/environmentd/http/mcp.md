---
source: src/environmentd/src/http/mcp.rs
revision: fb80db67fd
---

# environmentd::http::mcp

Implements Model Context Protocol (MCP) HTTP handlers that expose Materialize data products to AI agents via JSON-RPC 2.0.
Provides two endpoints: `/api/mcp/agents` (tools: `get_data_products`, `get_data_product_details`, `query`) for user data products discovered via `mz_internal.mz_mcp_data_products`, and `/api/mcp/observatory` (tool: `query_system_catalog`) for read-only access to `mz_*` system catalog tables.
Each endpoint is gated by a dynamic feature flag (`ENABLE_MCP_AGENTS`, `ENABLE_MCP_OBSERVATORY`, `ENABLE_MCP_AGENTS_QUERY_TOOL`).
Enforces read-only SQL validation and AST-based system-table access control before executing queries.
`McpRequestError` maps domain errors to standard JSON-RPC error codes.
