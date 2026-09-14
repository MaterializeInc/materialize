---
title: MCP Server Troubleshooting
description: "Troubleshooting guide for the Materialize MCP server."
make_table_row_headers_searchable: true
menu:
  main:
    parent: "mcp-server"
    name: "Troubleshoot MCP server"
    weight: 50
aliases:
  - /integrations/mcp-server/mcp-server-troubleshooting/
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
   default, and hosted clients such as Claude on the web reject them always.

**First, check your URL.** If you're using `http://`, try changing to
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

For an organization-wide rollout, configure your deployment with a certificate
from a trusted CA (for example, [Let's Encrypt](https://letsencrypt.org/)).
Hosted clients cannot disable verification.

## `HTTP 503 Service Unavailable`

**Symptom:** Requests to the MCP endpoint return HTTP 503.

**Cause:** The MCP endpoint is disabled by a system parameter.

**Fix:** Enable the endpoint. See [Environment-wide
configuration](/developer-tools/mcp-server/access-control/#environment-configuration).

## `HTTP 401 Unauthorized`

**Symptom:** Requests return HTTP 401.

**Cause:** Invalid or missing credentials. For OAuth, the access token may have
expired or been issued for a different audience. For a service account token,
the Base64 value may be incorrectly encoded, or the user or password may be
wrong.

**Fix:** For OAuth, reconnect the server in your client to sign in again. If the
failure persists on a Self-Managed deployment, see [OAuth sign-in fails
(Self-Managed)](#oauth-sign-in-fails-self-managed).

For a service account token, re-encode your credentials and verify:

```bash
# Encode
printf '<user>:<password>' | base64

# Verify by decoding
echo '<your-base64-token>' | base64 --decode
```

Make sure the decoded output matches `user:password` exactly.

## Permission denied on `query_system_tables` or a catalog query

**Symptom:** `query_system_tables` returns a permission error, or `query`
returns a permission error for a statement that references `mz_catalog`,
`mz_internal`, `pg_catalog`, or `information_schema`, while queries on user
objects succeed.

**Cause:** The role has `restrict_to_user_objects` set, which confines it to
user objects. This is the expected result for analyst roles.

**Fix:** If the role should be able to read the system catalog, a superuser
resets the parameter. The change takes effect on the next connection:

```mzsql
ALTER ROLE <role> RESET restrict_to_user_objects;
```

See [Confine a role to user
data](/developer-tools/mcp-server/access-control/#restrict-to-user-objects).

## `query` fails with a cluster permission error

**Symptom:** `query` returns an error that the role lacks `USAGE` on the
cluster, or that the cluster does not exist.

**Cause:** `query` runs on the cluster named in its `cluster` parameter, and the
role needs `USAGE` on it. The agent may have guessed a cluster name.

**Fix:** Ask the agent to call `get_permissions` to find the clusters the role
can use, or grant `USAGE` on the intended cluster:

```mzsql
GRANT USAGE ON CLUSTER <cluster> TO <role>;
```

## The agent uses a tool name the server does not recognize

**Symptom:** A `tools/call` for `get_data_products`, `query_system_catalog`,
or `read_data_product` fails, or an agent skill refers to a tool that is not
in `tools/list`.

**Cause:** These are legacy tool names. `get_data_products` and
`query_system_catalog` are accepted as aliases for `list_data_products` and
`query_system_tables` on every endpoint, but are not advertised.
`read_data_product` is deprecated.

**Fix:** Use the current names. See [Legacy tool
names](/developer-tools/mcp-server/tools/#legacy-tool-names). If the call fails
on an alias that should work, check that the client is sending the request to
the expected endpoint.

## Users do not see Materialize in their AI client

**Symptom:** An administrator added Materialize as an organization connector,
but a user does not see it, or sees it but cannot connect.

**Cause:** Depending on the client and plan, users may need to enable the
connector individually and complete a browser sign-in on first use. On
Self-Managed, the endpoint may not be reachable from the client vendor's
network.

**Fix:** Have the user open their client's connectors list and enable
Materialize (in Claude Code, type `/mcp`). On Self-Managed, confirm the endpoint
is reachable over HTTPS with a publicly trusted certificate. See [Running
behind enterprise
networks](/developer-tools/mcp-server/setup-self-managed/#running-behind-enterprise-networks).

## OAuth sign-in fails (Self-Managed)

**Symptom:** The browser sign-in fails at the identity provider (for example,
with a registration error or `invalid_scope`), or sign-in completes but the
client reports that the credentials were rejected on connect.

**Cause:** OAuth for Self-Managed deployments relies on your SSO identity
provider. Most enterprise IdPs need additional configuration for MCP clients,
such as a pre-registered OAuth client, an authentication claim in access
tokens, and the authorization server audience in `oidc_audience`.

**Fix:** See the [SSO troubleshooting
table](/security/self-managed/sso/#troubleshooting) for the specific symptoms
and resolutions, and the [Connecting MCP
clients](/security/self-managed/sso/#connecting-mcp-clients) section for the
full IdP configuration requirements.
