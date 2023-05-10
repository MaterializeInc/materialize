---
title: "DROP OWNED"
description: "`DROP OWNED` drops all the objects from your Materialize instance that are owned by one of the specified roles. Any privileges granted to the given roles on objects will also be revoked."
menu:
  main:
    parent: commands
---

`DROP OWNED` drops all the objects from your Materialize instance that are owned by one of the specified roles. Any privileges granted to the given roles on objects will also be revoked.

{{< note >}}
Unlike [PostgreSQL](https://www.postgresql.org/docs/current/sql-drop-owned.html), Materialize drops
all objects across all databases.
{{< /note >}}

## Syntax

{{< diagram "drop-owned.svg" >}}

Field | Use
------|-----
_role_name_   | The role name whose owned objects will be dropped.
**CASCADE** | Remove all dependent objects.
**RESTRICT**  | Don't remove anything if any non-index objects depencies exist. _(Default.)_

## Examples

```sql
DROP OWNED BY joe;
```

```sql
DROP OWNED BY joe, george CASCADE;
```

## Related pages

- [REVOKE PRIVILEGE](../revoke-privilege)
- [CREATE ROLE](../create-role)
- [DROP CLUSTER](../drop-cluster)
- [DROP CLUSTER REPLICA](../drop-cluster-replica)
- [DROP CONNECTION](../drop-connection)
- [DROP DATABASE](../drop-database)
- [DROP INDEX](../drop-index)
- [DROP MATERIALIZED VIEW](../drop-materialized-view)
- [DROP SCHEMA](../drop-schema)
- [DROP SECRET](../drop-secret)
- [DROP SINK](../drop-sink)
- [DROP SOURCE](../drop-source)
- [DROP TABLE](../drop-table)
- [DROP TYPE](../drop-type)
- [DROP VIEW](../drop-view)
