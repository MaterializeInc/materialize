---
source: src/environmentd/src/http/mcp.rs
revision: 4635519ab5
---

# environmentd::http::mcp

Implements Model Context Protocol (MCP) HTTP handlers that expose Materialize data products to AI agents via JSON-RPC 2.0.
Provides two endpoints: `/api/mcp/agents` (tools: `get_data_products`, `get_data_product_details`, `query`) for user data products, and `/api/mcp/observatory` (tool: `query_system_catalog`) for read-only access to `mz_*` system catalog tables.
Enforces read-only SQL validation and AST-based system-table access control before executing queries.
