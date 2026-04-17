---
title: "MCP Server"
description: "Learn how to integrate with Materialize's built-in MCP endpoints."
disable_list: true
menu:
  main:
    parent: integrations
    name: "MCP Server"
    identifier: mcp-server
    weight: 20
---

Materialize provides built-in Model Context Protocol (MCP) endpoints that AI
agents can use. The MCP interface is served directly by the database; no sidecar
process or external server is required.

## MCP endpoints overview

| Endpoint | Path | Description |
|----------|------|-------------|
| **Developer** | `/api/mcp/developer` | Read `mz_*` system catalog tables for troubleshooting and observability. <br>For details, see [MCP Server for developer](/integrations/mcp-server/mcp-developer/).|

## See also

- [MCP Server
  Troubleshooting](/integrations/mcp-server/mcp-server-troubleshooting/)
- [Appendix: MCP Server (Python)](/integrations/mcp-server/llm) for locally-run,
  separate MCP Server.
