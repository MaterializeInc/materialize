# mcp — Connect an AI agent to Materialize's developer MCP server

Exposes Materialize's developer MCP server to any MCP client (Claude Desktop,
Claude Code, Cursor) via the active profile, so the client can query the
Materialize system catalog without you having to manage credentials in the
client's own config.

For the full list of tools the developer MCP server provides and what they do,
see <https://materialize.com/docs/integrations/mcp-server/mcp-developer/>.

## Usage

    mz-deploy mcp

## Profile configuration

`mz-deploy mcp` requires the profile to set `http_host` — the Materialize HTTP
API hostname. It is separate from `host` (the SQL pgwire endpoint) because the
two aren't always on the same hostname. Profiles used only for MCP can omit
`host` entirely.

```toml
[prod]
http_host = "console.prod.example.materialize.com"
username  = "materialize_internal"
password  = "${MZ_PROFILE_PROD_PASSWORD}"

# Optionally also configure SQL for the same Materialize:
# host = "prod.example.materialize.com"
# port = 6875
```

`http_host` accepts a bare hostname, `host:port`, or a full URL with scheme.

## Wiring it into an MCP client

Most MCP clients accept a `mcpServers` config block. For Claude Code or
Claude Desktop:

    {
      "mcpServers": {
        "materialize-dev": {
          "command": "mz-deploy",
          "args": ["--profile", "prod", "mcp"]
        }
      }
    }

After restarting the client, Materialize's developer tools (e.g.
`query_system_catalog`) appear alongside the client's other tools.

## Error Recovery

- **`profile '…' has no HTTP host configured`** — Add `http_host = "..."` to
  the profile in `profiles.toml`.
- **HTTP 401 / authentication failed** — The server requires credentials.
  Add `password = "..."` to the profile (or set the matching
  `MZ_PROFILE_<NAME>_PASSWORD` env var).
- **HTTP 404** — The developer MCP endpoint is not enabled on the target
  environment. Confirm with the team that owns the deployment.

## Related Commands

- `mz-deploy sql` — Same profile, but for an interactive `psql` session.
- `mz-deploy debug` — Verifies profile credentials non-interactively.
- `mz-deploy profile list` — Shows configured profiles.
