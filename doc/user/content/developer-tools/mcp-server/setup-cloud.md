---
title: "Set up MCP for your organization on Cloud"
description: "Enable Materialize in your organization's AI clients once, so every user connects with their own identity and role."
make_table_row_headers_searchable: true
menu:
  main:
    parent: "mcp-server"
    name: "Set up on Cloud"
    weight: 10
    identifier: "mcp-server-setup-cloud"
---

{{< public-preview />}}

This guide is for **Organization Admins** of a Materialize Cloud organization.
By the end, Materialize appears as a connector in your organization's AI client,
every user connects with their own identity, and each user's session runs as
the database role their identity provider (IdP) group maps to. No user edits an
MCP configuration file.

The steps are:

1. [Map IdP groups to database roles](#step-1-map-idp-groups-to-database-roles)
1. [Grant privileges to the roles](#step-2-grant-privileges-to-the-roles)
1. [Get the MCP server URL](#step-3-get-the-mcp-server-url)
1. [Add Materialize to your AI client](#step-4-add-materialize-to-your-ai-client)
1. [Verify a user's access](#step-5-verify-a-users-access)

## Before you begin

- You are an **Organization Admin** in Materialize. Only admins can create
  service accounts, manage provisioning, and set `restrict_to_user_objects`.
- Your organization has an [SSO
  connection](/security/cloud/users-service-accounts/sso/) configured for your
  IdP. Users sign in to the MCP server with OAuth through this connection.
- To manage role membership from your IdP, your organization has [IdP group
  sync](/security/cloud/users-service-accounts/sync-idp-groups/) enabled. This
  is optional. Without it, you grant roles to users manually with `GRANT`.
- You administer an AI client that supports organization-wide remote MCP
  connectors, such as Claude Team or Enterprise.

## Step 1. Map IdP groups to database roles

Decide which groups of users get which level of access. A common split is
analysts, who read curated data products, and engineers, who also inspect the
system catalog. See [Reference
roles](/developer-tools/mcp-server/access-control/#reference-roles).

1. **In your IdP**, create a group per level of access, for example
   `mz_analyst` and `mz_engineer`, and assign users to them.

1. **Push the groups to Materialize** via SCIM. Follow [Sync identity provider
   groups to database
   roles](/security/cloud/users-service-accounts/sync-idp-groups/). Group sync
   assigns and unassigns role membership. It never creates roles.

1. **In Materialize**, create a database role with the **same name** as each
   group:

   ```mzsql
   CREATE ROLE mz_analyst;
   CREATE ROLE mz_engineer;
   ```

   When a user connects, Materialize grants them membership in the roles that
   match their groups. Membership is applied on connection, so a group change
   takes effect the next time the user connects.

If you are not using group sync, create the roles and grant them to users
directly:

```mzsql
GRANT mz_analyst TO "nate@example.com";
```

## Step 2. Grant privileges to the roles

Grant each role the clusters, schemas, and objects its members should reach,
and confine analyst roles to user data. For example:

```mzsql
GRANT USAGE ON CLUSTER mcp_cluster TO mz_analyst;
GRANT USAGE ON SCHEMA materialize.data_products TO mz_analyst;
GRANT SELECT ON ALL TABLES IN SCHEMA materialize.data_products TO mz_analyst;
ALTER ROLE mz_analyst SET restrict_to_user_objects = true;
ALTER ROLE mz_analyst SET cluster = mcp_cluster;

GRANT mz_analyst TO mz_engineer;
GRANT USAGE ON CLUSTER dev_cluster TO mz_engineer;
ALTER ROLE mz_engineer SET cluster = dev_cluster;
```

For what each statement does, how `restrict_to_user_objects` behaves, and how
to define curated data products, see [MCP access
controls](/developer-tools/mcp-server/access-control/).

## Step 3. Get the MCP server URL

The MCP server URL has the form:

```
https://<region-id>.materialize.cloud/api/mcp
```

To copy it from the Console:

1. Log in to the [Materialize Console](https://console.materialize.com/).
1. Click the **Connect** link (lower-left corner) to open the **Connect** modal
   and click the **MCP Server** tab.
1. Copy the MCP server URL.

{{< note >}}
**$TODO: Figure out ahead of launch.** Update the Console steps and add a
screenshot once the Connect modal shows the unified `/api/mcp` URL.
{{< /note >}}

## Step 4. Add Materialize to your AI client

{{< tabs >}}
{{< tab "Claude (Team or Enterprise)" >}}

1. In the Claude **Admin console**, go to **Organization settings** →
   **Connectors** → **Add** → **Custom**.

1. Paste the MCP server URL from [Step 3](#step-3-get-the-mcp-server-url) as
   the **Remote MCP server URL** and save.

   Claude discovers how to authenticate from the OAuth 2.0 Protected Resource
   Metadata ([RFC 9728](https://datatracker.ietf.org/doc/html/rfc9728)) that
   Materialize serves at `/.well-known/oauth-protected-resource`, and runs the
   OAuth flow against Materialize's authorization server, which is backed by
   your SSO connection.

1. Materialize now appears in every organization member's **Connectors** list.
   On first use, each user enables the connector (in Claude Code, by typing
   `/mcp`) and completes sign-in in their browser. From then on, their session
   runs as their own user and carries the roles their groups map to.

For the current steps for your plan, see Anthropic's [Get started with custom
connectors using Remote
MCP](https://support.claude.com/en/articles/11175166-get-started-with-custom-connectors-using-remote-mcp).

{{< note >}}
**$TODO: Figure out ahead of launch.** Confirm whether Claude's
enterprise-managed authorization (assigning the connector to IdP groups) removes
the per-user sign-in step, and document that path with Okta screenshots if so.
Anthropic's support docs currently describe each user connecting individually.
{{< /note >}}

{{< /tab >}}

{{< tab "Claude Code plugin" >}}

The Materialize plugin bundles the MCP server configuration and the
[Materialize agent skills](/developer-tools/mcp-server/coding-agent-skills/).

```
/plugin install materialize
```

On first run the plugin prompts for the MCP server URL from [Step
3](#step-3-get-the-mcp-server-url). Organizations with an internal plugin
registry can mirror the plugin with the organization's URL baked in, using
`extraKnownMarketplaces` and `enabledPlugins` in a checked-in
`.claude/settings.json`, so developers get it without any setup.

{{< note >}}
**$TODO: Figure out ahead of launch.** Confirm the plugin is published on the
official Claude Code marketplace, its final name, and how the URL is provided.
Until then, developers can add the server directly with `claude mcp add`, as
shown in the **Other clients** tab.
{{< /note >}}

{{< /tab >}}

{{< tab "ChatGPT" >}}

Add Materialize as a workspace custom connector using the MCP server URL from
[Step 3](#step-3-get-the-mcp-server-url).

{{< note >}}
**$TODO: Figure out ahead of launch.** Document the ChatGPT workspace custom
connector steps and any admin approval flow.
{{< /note >}}

{{< /tab >}}

{{< tab "Other clients" >}}

Any user can connect an MCP-compatible client to the organization's MCP server
URL directly. They sign in with OAuth and their session runs as their own role.

{{% include-headless "/headless/mcp-connect-clients" %}}

{{< /tab >}}
{{< /tabs >}}

## Step 5. Verify a user's access

Connect as a member of each group and confirm the session carries the expected
role:

1. In the AI client, ask: *What can I access in Materialize?* The agent calls
   `get_permissions` and `get_settings` and reports the role's grants, its
   default cluster, and whether `restrict_to_user_objects` is set.

1. Ask an analyst question, such as *What data products can I query?*, and an
   engineer question, such as *Why is my materialized view stale?*. An analyst
   with `restrict_to_user_objects` gets a permission error on the second
   question. That is the expected result.

1. In the Console SQL Shell, confirm the MCP sessions and the roles they ran as:

   ```mzsql
   SELECT authenticated_user, initial_application_name, connected_at
   FROM mz_internal.mz_session_history
   WHERE initial_application_name LIKE 'mz_mcp%'
   ORDER BY connected_at DESC
   LIMIT 20;
   ```

## Headless and CI access

Pipelines and autonomous agents cannot complete a browser sign-in. Give each
one a dedicated service account with an app password and connect with the
Base64-encoded `<user>:<app_password>` as an `Authorization: Basic` header.
Never use a personal account. See [Service
accounts](/developer-tools/mcp-server/access-control/#service-accounts).

## Network access

If your region uses [network
policies](/security/cloud/manage-network-policies/), hosted AI clients must be
allowed through. Network policies apply to the MCP endpoint the same way they
apply to SQL. See [Network
policies](/developer-tools/mcp-server/access-control/#network-policies).

## Related pages

- [MCP access controls](/developer-tools/mcp-server/access-control/)
- [Available tools](/developer-tools/mcp-server/tools/)
- [Sync identity provider groups to database roles](/security/cloud/users-service-accounts/sync-idp-groups/)
- [Troubleshoot the MCP server](/developer-tools/mcp-server/mcp-server-troubleshooting/)
