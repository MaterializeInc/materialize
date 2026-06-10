---
title: MCP Server Troubleshooting
description: "Troubleshooting guide for the MCP Server."
make_table_row_headers_searchable: true
menu:
  main:
    parent: "mcp-server"
    weight: 50
---

## `unable to verify the first certificate`

**Symptom:** Your MCP client (Claude Code, Cursor, etc.) returns an error like:

```
Error: SDK auth failed: unable to verify the first certificate
```

**Cause:** This error has two common causes:

1. **Wrong protocol:** You're using `http://` but your deployment has TLS
   enabled. Switch to `https://` in your MCP configuration.
2. **Self-signed certificate:** Your Materialize deployment uses a self-signed
   TLS certificate, which is the default for
   [self-managed installations](/self-managed-deployments/). MCP clients built
   on Node.js (including Claude Code) reject self-signed certificates by
   default.

**First, check your URL** — if you're using `http://`, try changing to
`https://`. If that resolves the error, update your MCP configuration.

**Fix:**

For **Claude Code**, start with TLS verification disabled:

```bash
NODE_TLS_REJECT_UNAUTHORIZED=0 claude
```

For **Cursor** or other Node.js-based clients, set the same environment variable
before launching:

```bash
export NODE_TLS_REJECT_UNAUTHORIZED=0
```

Alternatively, configure your deployment with a certificate from a trusted CA
(e.g., [Let's Encrypt](https://letsencrypt.org/)) to avoid this issue entirely.

## `HTTP 503 Service Unavailable`

**Symptom:** Requests to the MCP endpoint return HTTP 503.

**Cause:** The MCP endpoint is disabled.

**Fix:** Enable the endpoint. See
- [Developer endpoint
  configuration](/integrations/mcp-server/mcp-developer-config/)
- [Agents endpoint
  configuration](/integrations/mcp-server/mcp-developer-config/)

## `HTTP 401 Unauthorized`

**Symptom:** Requests return HTTP 401.

**Cause:** Invalid or missing credentials. The Base64 token may be incorrectly
encoded, or the user/password may be wrong.

**Fix:** Re-encode your credentials and verify:

```bash
# Encode
printf '<user>:<password>' | base64

# Verify by decoding
echo '<your-base64-token>' | base64 --decode
```

Make sure the decoded output matches `user:password` exactly.
