---
title: "MCP Servers and agent skills"
description: "This section contains guides for installing Materialize Agent skills and integrating with Materialize's built-in MCP servers."
disable_list: true
menu:
  main:
    name: "MCP servers"
    identifier: mcp-server
    parent: developer-tools
    weight: 10
aliases:
  - /integrations/mcp-server/llm/
  - /integrations/llm/
  - /integrations/mcp-server/
---


## Agent skills

Materialize provides open-source [agent
skills](https://github.com/MaterializeInc/agent-skills) that give coding agents
like Claude Code, Codex, and Cursor access to Materialize documentation and
reference material. For the list of available skills and installation
instructions, see [Agent Skills](/developer-tools/mcp-server/coding-agent-skills/).

## MCP servers

Materialize provides built-in Model Context Protocol (MCP) servers that AI
agents can use. The MCP interface is served directly by the database; no sidecar
process or external server is required. These endpoints use [JSON-RPC
 2.0](https://www.jsonrpc.org/specification) over HTTP POST (default port 6876)
and support the MCP `tools/list` and `tools/call` methods.

{{% include-headless "/headless/mcp-servers-table" %}}

### Protocol versions

The MCP servers support MCP protocol versions
[2025-11-25](https://modelcontextprotocol.io/specification/2025-11-25) and
[2026-07-28](https://modelcontextprotocol.io/specification/2026-07-28), and
pick the version for each request:

- **2025-11-25:** The client starts with `initialize`. Requests need no extra
  headers. A request without an `MCP-Protocol-Version` header is served as
  2025-11-25.
- **2026-07-28:** Available starting in v26.XX. There is no `initialize` step.
  The client can call `server/discover` to see the supported versions, and
  sends the version on every request: in the `MCP-Protocol-Version` header and
  in `params._meta`. The `Mcp-Method` header repeats the method, and on
  `tools/call` the `Mcp-Name` header repeats the tool name. A request whose
  headers do not match its body gets HTTP 400.

MCP clients pick the version. A client that supports both can try 2026-07-28
first and fall back to 2025-11-25. For example, a 2026-07-28 `tools/list`
request:

```bash
curl -X POST http://localhost:6876/api/mcp/developer \
  -H "Content-Type: application/json" \
  -H "MCP-Protocol-Version: 2026-07-28" \
  -H "Mcp-Method: tools/list" \
  -d '{
    "jsonrpc": "2.0",
    "id": 1,
    "method": "tools/list",
    "params": {
      "_meta": {
        "io.modelcontextprotocol/protocolVersion": "2026-07-28",
        "io.modelcontextprotocol/clientCapabilities": {}
      }
    }
  }'
```

To serve every request as 2025-11-25, set `enable_mcp_protocol_2026_07_28` to
`false`. See [Agent endpoint
configuration](/developer-tools/mcp-server/mcp-agent-config/) and [Developer
endpoint configuration](/developer-tools/mcp-server/mcp-developer-config/).

## See also

- [Use an ontology table](/transform-data/patterns/ontology/) to curate join
  relationships that agents query through the `query` tool before writing
  multi-table SQL.
- [MCP Server
  Troubleshooting](/developer-tools/mcp-server/mcp-server-troubleshooting/)
