---
title: "ALTER TYPE"
description: "`ALTER TYPE` changes properties of a type."
menu:
  main:
    parent: 'commands'
---

Use `ALTER TYPE` to:
- Rename a type.
- Change owner of a type.

## Syntax

{{< tabs >}}
{{< tab "Rename" >}}

### Rename

To rename a type:

```mzsql
ALTER TYPE <name> RENAME TO <new_name>;
```

Syntax element | Description
---------------|------------
`<name>`| The current name of the type.
`<new_name>`| The new name of the type.

See also [Renaming restrictions](/sql/identifiers/#renaming-restrictions).

{{< /tab >}}
{{< tab "Change owner" >}}

### Change owner

To change the owner of a type:

```mzsql
ALTER TYPE <name> OWNER TO <new_owner_role>;
```

Syntax element | Description
---------------|------------
`<name>`| The name of the type you want to change ownership of.
`<new_owner_role>`| The new owner of the type.

To change the owner of a type, you must be the current owner and have
membership in the `<new_owner_role>`.

{{< /tab >}}

{{< /tabs >}}

## Privileges

The privileges required to execute this statement are:

{{< include-md
file="shared-content/sql-command-privileges/alter-type.md" >}}
