---
headless: true
---

In the following, replace `<baseURL>` with your Materialize base URL. For
Cloud, the base URL has the form `https://<region-id>.materialize.cloud`. For
Self-Managed, it has the form `https://<host>:6876`.

{{< tabs >}}
{{< tab "Claude Code" >}}

1. Add the Materialize MCP server as a [local-scoped
   server](https://code.claude.com/docs/en/mcp#local-scope) (the configuration
   is stored in `~/.claude.json`):

   ```sh
   claude mcp add --transport http materialize <baseURL>/api/mcp
   ```

   For Self-Managed deployments whose identity provider requires a
   pre-registered OAuth client, add `--client-id` and `--callback-port`:

   ```sh
   claude mcp add --transport http materialize <baseURL>/api/mcp \
     --client-id <YOUR_CLIENT_ID> --callback-port 8080
   ```

   The `--callback-port` value must match the port in the
   `http://localhost:<port>/callback` redirect URI registered on the OAuth
   client. See [Connecting MCP
   clients](/security/self-managed/sso/#connecting-mcp-clients).

1. Restart Claude Code. On first connection, type `/mcp`, select
   **materialize**, and complete sign-in in your browser.

{{< /tab >}}

{{< tab "Claude (web, desktop, Chrome)" >}}

If your organization administrator has already added Materialize as an
organization connector, Materialize appears in your **Connectors** list. Enable
it and complete sign-in in your browser when prompted.

To add it yourself instead, add a custom connector. The exact steps depend on
your Claude plan; for example, **Customize** → **Connectors** → **+** → **Add
custom connector**. For the **Remote MCP server URL** field, enter
`<baseURL>/api/mcp`. See Anthropic's [Get started with custom connectors using
Remote
MCP](https://support.claude.com/en/articles/11175166-get-started-with-custom-connectors-using-remote-mcp)
guide for the steps for your plan.

{{< /tab >}}

{{< tab "Cursor" >}}

1. Add the Materialize MCP server entry to your local MCP settings file
   (`~/.cursor/mcp.json`). When merging into an existing `mcpServers` object,
   remember to add commas between entries.

   ```json {hl_lines="3-5"}
   {
     "mcpServers": {
       "materialize": {
         "url": "<baseURL>/api/mcp"
       }
     }
   }
   ```

1. Restart Cursor. On first connection, your browser opens to complete sign-in.

{{< /tab >}}

{{< tab "Generic HTTP" >}}

Any MCP-compatible client can connect by sending [JSON-RPC
2.0](https://www.jsonrpc.org/specification) requests over HTTP POST. The
server supports the MCP `initialize`, `tools/list`, and `tools/call` methods.

Clients discover how to authenticate through OAuth 2.0 Protected Resource
Metadata ([RFC 9728](https://datatracker.ietf.org/doc/html/rfc9728)), served at
`<baseURL>/.well-known/oauth-protected-resource` and at
`<baseURL>/.well-known/oauth-protected-resource/api/mcp`.

Once you hold an access token, list the available tools:

```bash
curl -X POST <baseURL>/api/mcp \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer <access-token>" \
  -d '{
    "jsonrpc": "2.0",
    "id": 1,
    "method": "tools/list"
  }'
```

For headless clients that cannot complete a browser sign-in, use a [service
account](/developer-tools/mcp-server/access-control/#service-accounts) and send
its Base64-encoded `<user>:<app_password>` credentials as `Authorization: Basic
<mcp-token>` instead.

{{< /tab >}}
{{< /tabs >}}
