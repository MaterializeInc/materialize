---
title: "Set up MCP for your organization on Self-Managed"
description: "Enable Materialize in your organization's AI clients on a Self-Managed deployment, with OAuth through your identity provider."
make_table_row_headers_searchable: true
menu:
  main:
    parent: "mcp-server"
    name: "Set up on Self-Managed"
    weight: 15
    identifier: "mcp-server-setup-self-managed"
---

{{< public-preview />}}

This guide is for administrators of a [Self-Managed
Materialize](/self-managed-deployments/) deployment. By the end, Materialize
appears as a connector in your organization's AI client, users sign in with
OAuth through your identity provider (IdP), and each session runs as the user's
own database role.

The MCP server is served by Materialize itself at `/api/mcp` on the same host
and port as the Console (default port 6876). The steps are:

1. [Choose an authorization server](#step-1-choose-an-authorization-server)
1. [Configure your IdP](#step-2-configure-your-idp)
1. [Create roles and grant privileges](#step-3-create-roles-and-grant-privileges)
1. [Get the MCP server URL](#step-4-get-the-mcp-server-url)
1. [Add Materialize to your AI client](#step-5-add-materialize-to-your-ai-client)
1. [Verify a user's access](#step-6-verify-a-users-access)

## Before you begin

- **TLS with a publicly trusted certificate.** OAuth requires TLS. Hosted AI
  clients such as Claude connect from the vendor's network and reject
  self-signed certificates, so the endpoint needs a certificate from a trusted
  CA. Local clients such as Claude Code also reject self-signed certificates by
  default. See [Troubleshooting](/developer-tools/mcp-server/mcp-server-troubleshooting/#unable-to-verify-the-first-certificate).
- **Reachability.** Hosted clients must be able to reach `https://<host>:6876`
  from the vendor's egress network. See [Running behind enterprise
  networks](#running-behind-enterprise-networks).
- **A superuser connection** to Materialize to create roles and set
  `restrict_to_user_objects`.

## Step 1. Choose an authorization server

MCP clients authenticate with OAuth access tokens. Materialize is the resource
server. It validates tokens and publishes OAuth 2.0 Protected Resource Metadata
([RFC 9728](https://datatracker.ietf.org/doc/html/rfc9728)) at
`/.well-known/oauth-protected-resource`, which tells clients which
authorization server to use. You have two options for that authorization
server.

{{< tabs >}}
{{< tab "Bundled enterprise auth (Ory)" >}}

Install Materialize with enterprise auth enabled. Ory Hydra (authorization
server) and Ory Kratos (identity) come up alongside Materialize. Dynamic Client
Registration ([RFC 7591](https://datatracker.ietf.org/doc/html/rfc7591)) is
enabled and PKCE is enforced for public clients, so MCP clients register
themselves without any pre-registration on your side. Materialize's resource
metadata points at the bundled Hydra issuer out of the box.

{{< note >}}
**$TODO: Figure out ahead of launch.** Document how to enable the bundled auth
server at install time (Helm values or Terraform variables), the Kratos OIDC
callback URI that the IdP application needs, and how Hydra is exposed on the
network. Also confirm whether Hydra supports Client ID Metadata Documents
(CIMD) from the 2026-07-28 MCP revision, or whether clients fall back to DCR.
{{< /note >}}

{{< /tab >}}
{{< tab "Existing SSO identity provider" >}}

If you already run [SSO](/security/self-managed/sso/) against your IdP, MCP
clients can authenticate against it directly. Most enterprise IdPs, including
Okta, do not allow anonymous Dynamic Client Registration, so you pre-register
a public OIDC client with PKCE and configure MCP clients with its client ID.
Follow [Connecting MCP clients](/security/self-managed/sso/#connecting-mcp-clients)
for the IdP and Materialize configuration, in particular:

- an authentication claim (such as `email`) in **access** tokens, not only ID
  tokens,
- a Materialize-dedicated audience added to `oidc_audience`,
- optionally the `mcp.read` and `offline_access` scopes.

With this option, hosted clients that require Dynamic Client Registration
cannot self-register against your IdP. Local clients that accept a
pre-registered client ID, such as Claude Code, work.

{{< /tab >}}
{{< /tabs >}}

## Step 2. Configure your IdP

1. **Create an OIDC application** for Materialize in your IdP (a standard OIDC
   web app in Okta). For the bundled auth server, the sign-in redirect URI is
   the Kratos OIDC callback. For an existing SSO IdP, follow [Connecting MCP
   clients](/security/self-managed/sso/#connecting-mcp-clients).

1. **Create groups** per level of access, for example `mz_analyst` and
   `mz_engineer`, and assign users to them.

1. **Assign the groups** to the application so their members can sign in.

{{< note >}}
**$TODO: Figure out ahead of launch.** SCIM provisioning and IdP group-to-role
mapping are not yet available on Self-Managed. Today each user maps 1:1 to a
Materialize role through a JWT claim (see the limitations listed in
[SSO](/security/self-managed/sso/)), and role membership is granted with
`GRANT`. Update this step, and Step 3, with the SCIM and group
sync procedure once they ship on Self-Managed.
{{< /note >}}

## Step 3. Create roles and grant privileges

Create a database role per level of access and grant it the clusters,
schemas, and objects its members should reach. Confine analyst roles to user
data. For example:

```mzsql
CREATE ROLE mz_analyst;
GRANT USAGE ON CLUSTER mcp_cluster TO mz_analyst;
GRANT USAGE ON SCHEMA materialize.data_products TO mz_analyst;
GRANT SELECT ON ALL TABLES IN SCHEMA materialize.data_products TO mz_analyst;
ALTER ROLE mz_analyst SET restrict_to_user_objects = true;
ALTER ROLE mz_analyst SET cluster = mcp_cluster;

CREATE ROLE mz_engineer;
GRANT mz_analyst TO mz_engineer;
GRANT USAGE ON CLUSTER dev_cluster TO mz_engineer;
ALTER ROLE mz_engineer SET cluster = dev_cluster;
```

Then grant the roles to users. Users are auto-provisioned as roles on first
SSO sign-in, named by the configured authentication claim:

```mzsql
GRANT mz_analyst TO "nate@example.com";
GRANT mz_engineer TO "tony@example.com";
```

For what each statement does and how `restrict_to_user_objects` behaves, see
[MCP access controls](/developer-tools/mcp-server/access-control/).

## Step 4. Get the MCP server URL

The MCP server URL has the form:

```
https://<host>:6876/api/mcp
```

`<host>` is the address of your deployment's `balancerd` load balancer:

- If [deployed via
  Terraform](/self-managed-deployments/installation/#install-using-terraform-modules),
  run the output command for your cloud provider:

  ```bash
  # AWS
  terraform output -raw nlb_dns_name

  # GCP
  terraform output -raw balancerd_load_balancer_ip

  # Azure
  terraform output -raw balancerd_load_balancer_ip
  ```

- If your deployment uses TLS and you can log in to the Console, click the
  **Connect** link (lower-left corner), open the **MCP Server** tab, and copy
  the URL.

- For a local
  [kind](/self-managed-deployments/installation/install-on-local-kind/)
  cluster, port-forward `balancerd` and use `localhost` as the host. Hosted AI
  clients cannot reach a local cluster. Use a local client such as Claude Code.

  ```bash
  kubectl port-forward svc/<instance-name>-balancerd 6876:6876 -n materialize-environment
  ```

## Step 5. Add Materialize to your AI client

{{< tabs >}}
{{< tab "Claude (Team or Enterprise)" >}}

1. In the Claude **Admin console**, go to **Organization settings** →
   **Connectors** → **Add** → **Custom**.

1. Paste the MCP server URL from [Step 4](#step-4-get-the-mcp-server-url) as
   the **Remote MCP server URL** and save.

   Claude reads Materialize's resource metadata, discovers the authorization
   server, and, with the bundled auth server, registers itself via Dynamic
   Client Registration.

1. Confirm the endpoint is reachable from Claude's egress network over HTTPS
   with a publicly trusted certificate. If the connector fails to save, see
   [Running behind enterprise networks](#running-behind-enterprise-networks).

1. Materialize now appears in every organization member's **Connectors** list.
   On first use, each user enables the connector and completes sign-in through
   your IdP in their browser. Their session runs as their own role.

For the current steps for your plan, see Anthropic's [Get started with custom
connectors using Remote
MCP](https://support.claude.com/en/articles/11175166-get-started-with-custom-connectors-using-remote-mcp).

{{< /tab >}}

{{< tab "Claude Code plugin" >}}

The Materialize plugin bundles the MCP server configuration and the
[Materialize agent skills](/developer-tools/mcp-server/coding-agent-skills/).

```
/plugin install materialize
```

On first run the plugin prompts for the MCP server URL from [Step
4](#step-4-get-the-mcp-server-url). Organizations with an internal plugin
registry can mirror the plugin with the organization's URL baked in, using
`extraKnownMarketplaces` and `enabledPlugins` in a checked-in
`.claude/settings.json`.

{{< note >}}
**$TODO: Figure out ahead of launch.** Confirm the plugin is published on the
official Claude Code marketplace and how a pre-registered client ID is
supplied when the IdP does not support Dynamic Client Registration.
{{< /note >}}

{{< /tab >}}

{{< tab "Other clients" >}}

Any user can connect an MCP-compatible client to the MCP server URL directly.
They sign in with OAuth through your IdP and their session runs as their own
role.

{{% include-headless "/headless/mcp-connect-clients" %}}

{{< /tab >}}
{{< /tabs >}}

## Step 6. Verify a user's access

1. In the AI client, ask: *What can I access in Materialize?* The agent calls
   `get_permissions` and `get_settings` and reports the role's grants, its
   default cluster, and whether `restrict_to_user_objects` is set.

1. Ask an analyst question, such as *What data products can I query?*, and an
   engineer question, such as *Why is my materialized view stale?*. An analyst
   with `restrict_to_user_objects` gets a permission error on the second
   question. That is the expected result.

1. Confirm the MCP sessions and the roles they ran as:

   ```mzsql
   SELECT authenticated_user, initial_application_name, connected_at
   FROM mz_internal.mz_session_history
   WHERE initial_application_name LIKE 'mz_mcp%'
   ORDER BY connected_at DESC
   LIMIT 20;
   ```

## Running behind enterprise networks

Hosted AI clients connect to your deployment from the vendor's network, so the
MCP endpoint must be reachable through your perimeter.

- **Paths to allow.** Clients need `POST /api/mcp` and `GET
  /.well-known/oauth-protected-resource` (also served with the `/api/mcp`
  suffix). Clients using the bundled auth server also need to reach the Hydra
  issuer's OAuth endpoints. Clients on the legacy endpoints use
  `/api/mcp/agent` and `/api/mcp/developer`.
- **Source addresses.** Allow the vendor's egress ranges through your WAF and
  firewall, and in any Materialize [network
  policy](/developer-tools/mcp-server/access-control/#network-policies).
- **Load balancer.** The MCP server runs over plain HTTPS POST with JSON bodies
  on the same `balancerd` listener as the Console. Deployments behind a proxy
  that rewrites the `Host` header must ensure the resource metadata still
  advertises the externally reachable URL.

{{< note >}}
**$TODO: Figure out ahead of launch.** Add the authoritative egress IP ranges
for claude.ai and ChatGPT, a reference WAF allowlist, load balancer timeout and
body size recommendations, and the exact list of auth server paths to expose
for the bundled Ory deployment.
{{< /note >}}

## Headless and CI access

Pipelines and autonomous agents cannot complete a browser sign-in. Give each
one a dedicated login role with a password and connect with the Base64-encoded
`<user>:<password>` as an `Authorization: Basic` header. Never use a personal
account. See [Service
accounts](/developer-tools/mcp-server/access-control/#service-accounts).

## Emulator

The [Materialize Emulator](/developer-tools/install-materialize-emulator/)
serves the same MCP server at `http://localhost:6876/api/mcp` and does not
require authentication. Unauthenticated requests run as the
`anonymous_http_user` role. To run as a specific role, pass its credentials as
a Basic auth token as described in [Service
accounts](/developer-tools/mcp-server/access-control/#service-accounts).

## Related pages

- [MCP access controls](/developer-tools/mcp-server/access-control/)
- [Available tools](/developer-tools/mcp-server/tools/)
- [Single sign-on (SSO) for Self-Managed](/security/self-managed/sso/)
- [Troubleshoot the MCP server](/developer-tools/mcp-server/mcp-server-troubleshooting/)
