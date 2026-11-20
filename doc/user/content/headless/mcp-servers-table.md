---
headless: true
---
| Endpoint | Path | Description |
|----------|------|-------------|
| **MCP server** | `/api/mcp` | Discover and query your data products, and read the system catalog, from one server. Recommended for new setups. <br>For details, see [MCP Server](/developer-tools/mcp-server/materialize-mcp/).<br>*Available starting in v26.NN*|
| **Agent** | `/api/mcp/agent` | Discover and query your real-time data products over HTTP. <br>For details, see [MCP Server for agents](/developer-tools/mcp-server/mcp-agent/).<br>*Available starting in v26.24*|
| **Developer** | `/api/mcp/developer` | Read `mz_*` system catalog tables for troubleshooting and observability, and run queries on your objects. <br>For details, see [MCP Server for developer](/developer-tools/mcp-server/mcp-developer/). <br>*Available starting in v26.20*|
