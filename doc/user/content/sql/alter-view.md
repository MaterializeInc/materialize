---
title: "ALTER VIEW"
description: "`ALTER VIEW` changes properties of a view."
menu:
  main:
    parent: 'commands'
---

Use `ALTER VIEW` to:
- Rename a view.
- Change owner of a view.

## Syntax

{{< tabs >}}
{{< tab "Rename" >}}

### Rename

To rename a view:

```mzsql
ALTER VIEW <name> RENAME TO <new_name>;
```

Syntax element | Description
---------------|------------
`<name>`| The current name of the view.
`<new_name>`| The new name of the view.

See also [Renaming restrictions](/sql/identifiers/#renaming-restrictions).

{{< /tab >}}
{{< tab "Change owner" >}}

### Change owner

To change the owner of a view:

```mzsql
ALTER VIEW <name> OWNER TO <new_owner_role>;
```

Syntax element | Description
---------------|------------
`<name>`| The name of the view you want to change ownership of.
`<new_owner_role>`| The new owner of the view.

To change the owner of a view, you must be the current owner and have
membership in the `<new_owner_role>`.

{{< /tab >}}

{{< /tabs >}}

## Privileges

The privileges required to execute this statement are:

{{< include-md
file="shared-content/sql-command-privileges/alter-view.md" >}}
