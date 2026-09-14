---
title: "MCP access controls"
description: "Govern what agents can read through the Materialize MCP server using the roles, grants, and role settings you already have."
make_table_row_headers_searchable: true
menu:
  main:
    parent: "mcp-server"
    name: "Control MCP access"
    weight: 20
    identifier: "mcp-server-access-control"
aliases:
  - /integrations/mcp-server/mcp-agent-config/
  - /integrations/mcp-server/mcp-developer-config/
  - /developer-tools/mcp-server/mcp-agent-config/
  - /developer-tools/mcp-server/mcp-developer-config/
---

{{< public-preview />}}

The Materialize MCP server does not have its own permission system. Every tool
call runs as the authenticated user's database role and is checked, on every
call, against the same [role-based access control (RBAC)](/security/) grants
that govern SQL and the Console. SQL, the Console, and MCP all obey the same
`GRANT`.

This page explains the controls you have, how they combine, and how to check
what a role ends up with.

## Three layers of control

| Who | Scope | Control |
|-----|-------|---------|
| **Operator** | Whole environment | [System parameters](#environment-configuration) turn the MCP endpoint and individual tools on or off for everyone. |
| **Administrator** | Per role | [`ALTER ROLE ... SET restrict_to_user_objects`](#restrict-to-user-objects) confines a role to user data. `ALTER ROLE ... SET cluster` picks the cluster tools use by default. |
| **Administrator** | Per object | The [grants](/sql/grant-privilege/) you already write: `SELECT` on what the role should read, `USAGE` on the schemas and clusters it should use. |

Access is per object, not per tool. Two roles cannot be given different tool
sets. A role can call any tool, and each call returns what the role's grants
allow. To give an analyst a narrow agent, confine the role with
`restrict_to_user_objects` and grant it only the objects it should see.

## What each tool needs

| Tool | Privileges checked at call time |
|------|----------------------------------|
| `get_permissions`, `get_settings` | None beyond a successful connection. Both work for restricted roles. |
| `list_data_products` | Results are filtered to objects the role has `SELECT` on. |
| `get_data_product_details` | `SELECT` on the data product. |
| `query` | `SELECT` on each referenced object, `USAGE` on its schema, `USAGE` on the named cluster. Catalog references are refused when `restrict_to_user_objects` is set. |
| `query_system_tables` | `SELECT` on the referenced catalog relations, which most roles hold by default. Refused when `restrict_to_user_objects` is set. |

All tools are read-only. Write statements are rejected before any grant is
consulted.

## Reference roles

Most organizations need two kinds of agent access: analysts who read curated
data products, and engineers who also inspect the system and run `EXPLAIN`. The
following roles implement that split. Adjust cluster and schema names to match
your environment.

```mzsql
-- The analyst: reads curated data products, nothing else.
CREATE ROLE analyst;
GRANT USAGE ON CLUSTER mcp_cluster TO analyst;
GRANT USAGE ON SCHEMA materialize.data_products TO analyst;
GRANT SELECT ON ALL TABLES IN SCHEMA materialize.data_products TO analyst;
ALTER ROLE analyst SET restrict_to_user_objects = true;
ALTER ROLE analyst SET cluster = mcp_cluster;

-- The engineer: everything the analyst has, plus the system catalog and a
-- development cluster for EXPLAIN ANALYZE.
CREATE ROLE engineer;
GRANT analyst TO engineer;
GRANT USAGE ON CLUSTER dev_cluster TO engineer;
ALTER ROLE engineer SET cluster = dev_cluster;
```

What each statement does:

- `GRANT USAGE ON CLUSTER` decides where the role's queries may run.
  `query` requires `USAGE` on the cluster it names.
- `GRANT USAGE ON SCHEMA` and `GRANT SELECT ON ALL TABLES IN SCHEMA` decide
  what the role may read. `TABLES` includes views and materialized views.
- `ALTER ROLE ... SET restrict_to_user_objects = true` hides the system catalog
  from the analyst. Because `engineer` does not set it, the engineer can use
  `query_system_tables`.
- `ALTER ROLE ... SET cluster` sets the default cluster the role connects with.
  Role settings are **not** inherited through `GRANT analyst TO engineer`. Only
  privileges are inherited, so each role sets its own defaults.

To grant `SELECT` on future data products automatically, add a [default
privilege](/sql/alter-default-privileges/):

```mzsql
ALTER DEFAULT PRIVILEGES
  FOR ROLE <creator_role>
  IN SCHEMA materialize.data_products
  GRANT SELECT ON TABLES TO analyst;
```

When users sign in through your identity provider, map their IdP groups to
these roles so that membership is managed in the IdP. See [Set up MCP for your
organization on Cloud](/developer-tools/mcp-server/setup-cloud/) and [on
Self-Managed](/developer-tools/mcp-server/setup-self-managed/).

## Confine a role to user data {#restrict-to-user-objects}

By default, a role can read the system catalog (`mz_catalog`, `mz_internal`,
`pg_catalog`, `information_schema`) because most catalog relations grant
`SELECT` to `PUBLIC`, the same way PostgreSQL makes `pg_catalog` readable by
everyone. Drivers and tools depend on that, so it cannot be revoked with an
ordinary `REVOKE`.

To hide the catalog from an agent role, a **superuser** sets
`restrict_to_user_objects` on the role:

```mzsql
ALTER ROLE analyst SET restrict_to_user_objects = true;
```

Once set:

- The role can read user objects it has `SELECT` on, plus the MCP data product
  views that back `list_data_products` and `get_data_product_details`.
- `query_system_tables` is refused, and any `query` that references a system
  catalog object is refused with a permission error.
- `get_permissions` and `get_settings` continue to work, so the agent can still
  learn what it may do.
- The role cannot lift the restriction. Only a superuser can set or reset the
  parameter, and a plain `SET restrict_to_user_objects` in a session is
  refused. This also means SQL written by the agent cannot lift it.
- The setting takes effect on the role's next connection.

Role settings are not inherited, so set the parameter on each role that should
be confined, not only on a parent role.

To remove the restriction:

```mzsql
ALTER ROLE analyst RESET restrict_to_user_objects;
```

{{< note >}}
The reverse, a role that can read the catalog but no user data, needs no
special setting. A read of a user object needs `USAGE` on the cluster it runs
on, so a role with no cluster grants reaches no user data. Catalog-only
statements are routed to `mz_catalog_server`, which grants `USAGE` to `PUBLIC`.
If your environment granted `USAGE` on a default cluster to `PUBLIC` at
creation, revoke it first with `REVOKE USAGE ON CLUSTER quickstart FROM
PUBLIC`, which affects every role in the environment.
{{< /note >}}

## Define data products {#define-data-products}

An agent's view of your data is only as good as the objects you expose to it.
Rather than granting `SELECT` on production objects directly, create curated
data products in a dedicated schema and cluster:

1. Create a dedicated cluster and schema:

   ```mzsql
   CREATE CLUSTER mcp_cluster SIZE '25cc';
   CREATE SCHEMA materialize.data_products;
   ```

1. Create a view over the object you want to expose, projecting, masking, or
   filtering as needed. If the underlying object is a materialized view, an
   index on the new view reuses the maintained result instead of recomputing
   it:

   ```mzsql
   CREATE VIEW materialize.data_products.product_performance AS
   SELECT product_id, total_revenue, stock_status
   FROM sales.product_performance;

   CREATE INDEX product_performance_idx
   IN CLUSTER mcp_cluster
   ON materialize.data_products.product_performance (product_id);
   ```

   The indexed columns are surfaced to the agent as preferred lookup keys.

1. Add comments. They are surfaced to the agent to help it understand when and
   how to use the data product:

   ```mzsql
   COMMENT ON VIEW materialize.data_products.product_performance IS
   'Per-product sales performance and stock status. Use this for questions
   about a specific product''s revenue or inventory.';

   COMMENT ON COLUMN materialize.data_products.product_performance.total_revenue IS
   'Lifetime gross revenue for this product.';
   ```

   If the index also has a comment, the index's comment is surfaced instead of
   the view's. Column comments go on the view.

1. Grant access as shown in [Reference roles](#reference-roles).

{{< tip >}}
Because `query` can join across objects, consider maintaining an [ontology
table](/transform-data/patterns/ontology/) that lists the join relationships
in your schema, so the agent can confirm exact join keys before writing
multi-table SQL.
{{< /tip >}}

## Check what a role can do

You do not need to read source code or trace grants by hand to answer "what can
this person do through MCP". Connect as the role and call the two reporting
tools:

- `get_permissions` returns the privileges the role holds, including inherited
  ones.
- `get_settings` returns the effective `restrict_to_user_objects` value and
  default cluster.

Both work for restricted roles. In a SQL session you can get the same answer
from `mz_internal.mz_show_all_my_privileges` and `SHOW restrict_to_user_objects`
(for unrestricted roles), or from the Console.

## Service accounts {#service-accounts}

Interactive users should connect with OAuth so that each session carries their
own identity and role. For autonomous agents, CI pipelines, and other headless
clients that cannot complete a browser sign-in, use a dedicated **service
account** instead of a personal account:

- Create a dedicated login role for the agent, and grant it a functional role
  such as `analyst`. Never reuse a person's credentials. A personal account
  carries all of that person's privileges.
- Set `restrict_to_user_objects = true` on the service account's role unless
  the agent needs the system catalog.
- Attach a [network policy](#network-policies) so the account can only connect
  from where the agent runs.
- Authenticate with the Base64-encoded `<user>:<app_password>` as an
  `Authorization: Basic <mcp-token>` header. Keep the token out of version
  control.

{{< tabs >}}
{{< tab "Cloud" >}}

In the [Materialize Console](https://console.materialize.com/), an
Organization Admin creates a [service
account](/security/cloud/users-service-accounts/create-service-accounts/) via
**+ Create New** → **App Password** with type **Service**. The Console shows
the **MCP Token** once, at creation time. Then grant the functional role:

```mzsql
GRANT analyst TO my_agent;
ALTER ROLE my_agent SET cluster = mcp_cluster;
ALTER ROLE my_agent SET restrict_to_user_objects = true;
```

{{< /tab >}}
{{< tab "Self-Managed" >}}

```mzsql
CREATE ROLE my_agent LOGIN PASSWORD '<your_app_password>';
GRANT analyst TO my_agent;
ALTER ROLE my_agent SET cluster = mcp_cluster;
ALTER ROLE my_agent SET restrict_to_user_objects = true;
```

Generate the MCP token:

```bash
printf 'my_agent:<your_app_password>' | base64
```

{{< /tab >}}
{{< tab "Emulator" >}}

The Emulator does not require authentication. Unauthenticated requests run as
the `anonymous_http_user` role. To run as a specific role, create it and pass
its credentials without a password:

```mzsql
CREATE ROLE my_agent;
```

```bash
printf 'my_agent:' | base64
```

{{< /tab >}}
{{< /tabs >}}

## Network policies {#network-policies}

[Network policies](/security/cloud/manage-network-policies/) are enforced at the
database layer and apply to HTTP connections, including the MCP endpoint, the
same way they apply to SQL. If you restrict ingress by IP, hosted AI clients
must be allowed through:

- Claude (web and desktop) connects from Anthropic's egress addresses. Allow
  those ranges in your network policy.
- Local clients such as Claude Code and Cursor connect from the user's machine
  or corporate network.

{{< note >}}
**$TODO: Figure out ahead of launch.** Link to the authoritative egress IP
ranges for claude.ai and ChatGPT connectors, and add an example network policy
that allows them.
{{< /note >}}

## Environment-wide configuration {#environment-configuration}

System parameters turn the MCP endpoint and individual tools on or off for the
whole environment. They apply to everyone and are not a substitute for role
grants.

| Parameter | Default | Description |
|-----------|---------|-------------|
| `enable_mcp_agent` | `true` | Enable or disable the legacy `/api/mcp/agent` endpoint. When disabled, requests return `HTTP 503`. |
| `enable_mcp_developer` | `true` | Enable or disable the legacy `/api/mcp/developer` endpoint. When disabled, requests return `HTTP 503`. |
| `enable_mcp_agent_query_tool` | `true` | Enable or disable the `query` tool on the legacy `/api/mcp/agent` endpoint. |
| `enable_mcp_developer_query_tool` | `true` | Enable or disable the `query` tool on the legacy `/api/mcp/developer` endpoint. |
| `mcp_max_response_size` | `1000000` | Maximum response size in bytes. Queries exceeding this limit return an error. |

{{< note >}}
**$TODO: Figure out ahead of launch.** Add the parameters that control the
unified `/api/mcp` endpoint and its tools (names and defaults), and confirm
whether the per-tool flags above also apply to `/api/mcp`.
{{< /note >}}

To change a parameter:

{{< tabs >}}
{{< tab "Cloud" >}}

Contact [Materialize support](https://materialize.com/docs/support/) to change
MCP system parameters for your environment.

{{< /tab >}}
{{< tab "Self-Managed" >}}

Use one of the following methods.

**Configuration file.** Set the parameter in your [system parameters
configuration file](/self-managed-deployments/configuration-system-parameters/):

```yaml
system_parameters:
  mcp_max_response_size: "500000"
```

**Terraform.** Set the parameter via the [Materialize Terraform
module](https://github.com/MaterializeInc/materialize-terraform-self-managed):

```hcl
system_parameters = {
  mcp_max_response_size = "500000"
}
```

**SQL.** Connect as `mz_system` and run:

```mzsql
ALTER SYSTEM SET mcp_max_response_size = 500000;
```

These parameters are only accessible to the `mz_system` and `mz_support` roles.

{{< /tab >}}
{{< /tabs >}}

## Audit trail

Administrators need to answer "which agents read what, on whose behalf, and
when". Today you can reconstruct this from existing sources:

- **Sessions.** MCP sessions set `application_name`, so you can find them by `initial_application_name` in
  `mz_internal.mz_session_history`. Legacy endpoints use `mz_mcp_agents` and
  `mz_mcp_developer`.
- **Statements.** SQL run by tool calls appears in the [statement
  log](/sql/system-catalog/mz_internal/#mz_statement_execution_history) like any
  other statement, joined to the session by `session_id`. Note that statement
  logging is sampled and rate limited under load.
- **Metrics.** The `mz_mcp_requests_total`, `mz_mcp_tool_calls_total`, and
  `mz_mcp_tool_call_duration_seconds` Prometheus metrics are labeled by
  endpoint and tool. See the [metrics appendix](/observability/appendix-metrics/).

{{< note >}}
**$TODO: Figure out ahead of launch.** Document the `application_name` value
used by the unified `/api/mcp` endpoint, and document the
`mz_internal.mz_mcp_tool_calls` audit table (columns, retention, example
queries) once it ships. The PRD proposes columns for timestamp, user and role,
client, endpoint, tool, arguments, session and statement IDs, and outcome, with
retention matching the statement log.
{{< /note >}}

## What you cannot do

- **Grant or revoke a tool.** There is no `GRANT` on an MCP tool. Access is per
  object, so two roles cannot be given different tool sets. A role that should
  not read the system catalog gets `restrict_to_user_objects`, not a hidden
  tool.
- **Hide tools from `tools/list`.** The list is the same for every role. This is
  deliberate: the catalog is readable by `PUBLIC` for PostgreSQL compatibility,
  so grants alone cannot express "this role may not read the catalog", and
  hiding a tool would not protect anything since permissions are enforced when
  the tool runs.
- **Write through MCP.** All tools are read-only, regardless of the role's
  `CREATE` or `INSERT` privileges.
- **Control access from the client.** URL parameters or client-side flags are
  not honored as access controls. Only server-side grants and role settings
  apply, so an organization can enforce them.
