---
title: "ALTER DATABASE"
description: "`ALTER DATABASE` changes properties of a database."
menu:
  main:
    parent: 'commands'
---

Use `ALTER DATABASE` to:
- Rename a database.
- Change owner of a database.

## Syntax

{{< tabs >}}
{{< tab "Rename" >}}

### Rename

To rename a database:

```mzsql
ALTER database <name> RENAME TO <new_name>;
```

Syntax element | Description
---------------|------------
`<name>`| The current name of the database.
`<new_name>`| The new name of the database.

See also [Renaming restrictions](/sql/identifiers/#renaming-restrictions).

{{< /tab >}}
{{< tab "Change owner" >}}

### Change owner

To change the owner of a database:

```mzsql
ALTER database <name> OWNER TO <new_owner_role>;
```

Syntax element | Description
---------------|------------
`<name>`| The name of the database you want to change ownership of.
`<new_owner_role>`| The new owner of the database.

To change the owner of a database, you must be the current owner and have
membership in the `<new_owner_role>`.

{{< /tab >}}

{{< /tabs >}}

## Privileges

The privileges required to execute this statement are:

{{< include-md
file="shared-content/sql-command-privileges/alter-database.md" >}}
