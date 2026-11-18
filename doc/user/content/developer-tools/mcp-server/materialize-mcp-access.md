---
title: Limit MCP access by role
description: "Use grants and the restrict_to_user_objects role setting to control what each role can read through the /api/mcp MCP server."
menu:
  main:
    parent: "mcp-server-unified"
    weight: 20
    identifier: "unified-endpoint-access"
    name: "Limit access by role"
---

The [`materialize-mcp` server](/developer-tools/mcp-server/materialize-mcp/)
shows the same three tools to every role. It does not hide tools based on the
role. Instead, each tool call runs as the connected role, and Materialize
checks the role's privileges when the call runs. A call that the role is not
allowed to make returns an error.

You control what a role can read with:

- **Grants**: `SELECT` on the objects the role should read, `USAGE` on their
  schemas, and `USAGE` on the clusters where its queries run.
- **The `restrict_to_user_objects` role setting**: Blocks the role from reading
  the system catalog.

By default, most system catalog objects are readable by every role. So without
`restrict_to_user_objects`, a role can use the
[`query`](/developer-tools/mcp-server/materialize-mcp-tools/#query) tool to read
the system catalog, for example cluster names, role names, and source errors.

## What `restrict_to_user_objects` does

When a role has `restrict_to_user_objects` set to `true`:

- Queries that reference system catalog objects fail with an error like
  `access to system object mz_clusters is restricted`. This includes objects in
  `mz_catalog`, `mz_internal`, `mz_introspection`, `pg_catalog`, and
  `information_schema`.
- `SHOW` commands that list objects, such as `SHOW CLUSTERS`, also fail,
  because they read the system catalog.
- `list_data_products` and `get_data_product_details` still work.
- `query` still reads the user objects that the role has privileges on.
- Only a superuser can set or remove the setting. The role cannot turn it off
  for itself, and neither can a query that an agent writes. In Cloud, an
  Organization Admin is a superuser.

The setting applies to new sessions. Role settings are **not** inherited
through role membership, so set it on each role that connects.

Some built-in functions also read the system catalog and fail under the
restriction, for example `pg_has_role`. See [Restricting `query` tool access to
user objects
only](/developer-tools/mcp-server/mcp-agent-tools/#restrict-to-user-objects).

## Example: an analyst and an engineer

In this example, two roles connect to the same `/api/mcp` server:

- The `analyst` role reads curated data products only.
- The `engineer` role reads the same data products, and can also read the
  system catalog to troubleshoot.

Both roles share their grants through a functional role. Only the analyst is
restricted.

The example assumes the data product
`materialize.mcp_schema.mcp_product_performance` exists on the cluster
`mcp_cluster`. To create one, see [Define data products and grant
access](/developer-tools/mcp-server/mcp-agent/#define-data-products-and-grant-access).

1. Create a functional role and grant it access to the data products:

   ```mzsql
   CREATE ROLE data_reader;
   GRANT USAGE ON CLUSTER mcp_cluster TO data_reader;
   GRANT USAGE ON SCHEMA materialize.mcp_schema TO data_reader;
   GRANT SELECT ON materialize.mcp_schema.mcp_product_performance TO data_reader;
   ```

1. Grant the functional role to the analyst and the engineer:

   ```mzsql
   GRANT data_reader TO analyst;
   GRANT data_reader TO engineer;
   ```

   Here, `analyst` and `engineer` stand for the roles that your users or
   service accounts connect as. With OAuth, this is each user's own role.

1. Set the default cluster on each role. Role settings are not inherited, so
   set them on each role directly:

   ```mzsql
   ALTER ROLE analyst SET cluster TO mcp_cluster;
   ALTER ROLE engineer SET cluster TO mcp_cluster;
   ```

1. As a **superuser**, restrict the analyst to user objects:

   ```mzsql
   ALTER ROLE analyst SET restrict_to_user_objects = true;
   ```

1. Check the setting:

   ```mzsql
   SELECT r.name, p.parameter_value
   FROM mz_catalog.mz_role_parameters AS p
   JOIN mz_catalog.mz_roles AS r ON r.id = p.role_id
   WHERE p.parameter_name = 'restrict_to_user_objects';
   ```

### What each role can do

| Request | `analyst` | `engineer` |
|---------|-----------|------------|
| `list_data_products` | Lists `mcp_product_performance`. | Lists `mcp_product_performance`. |
| `query` on `mcp_product_performance` | Works. | Works. |
| `query` on `mz_internal.mz_source_statuses` | Fails. | Works. |
| `query` on `mz_introspection` relations, with `cluster` set to `mcp_cluster` | Fails. | Works. |
| `SHOW CLUSTERS` | Fails. | Works. |

The engineer reads `mz_introspection` relations only on clusters where it has
`USAGE`. Neither role can write data, because every tool is read-only.

### Remove the restriction

As a superuser, reset the setting:

```mzsql
ALTER ROLE analyst RESET restrict_to_user_objects;
```

## Related pages

- [Available tools](/developer-tools/mcp-server/materialize-mcp-tools/)
- [ALTER ROLE](/sql/alter-role/)
- [GRANT PRIVILEGE](/sql/grant-privilege/)
- [Fine-grained access control](/security/fine-grained-access-control/)
