---
title: "REASSIGN OWNED"
description: "`REASSIGN OWNED` reassigns the owner of all the objects that are owned by one of the specified roles."
menu:
  main:
    parent: commands
---

`REASSIGN OWNED` reassigns the owner of all the objects that are owned by one of the specified roles.

{{< note >}}
Unlike [PostgreSQL](https://www.postgresql.org/docs/current/sql-drop-owned.html), Materialize reassigns
all objects across all databases, including the databases themselves.
{{< /note >}}

## Syntax

```mzsql
REASSIGN OWNED BY <current_owner> [, ...] TO <new_owner>;
```

Syntax element | Description
---------------|------------
`<current_owner>` | The role whose objects are to be reassigned.
`<new_owner>` | The role name of the new owner of these objects.

## Examples

```mzsql
REASSIGN OWNED BY joe TO mike;
```

```mzsql
REASSIGN OWNED BY joe, george TO mike;
```

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/reassign-owned.md"
>}}

## Related pages

- [`ALTER OWNER`](/sql/#rbac)
- [`DROP OWNED`](../drop-owned)
