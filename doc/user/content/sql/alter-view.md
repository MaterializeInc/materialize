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

{{% include-syntax file="examples/alter_view" example="syntax-rename" %}}

{{< /tab >}}
{{< tab "Change owner" >}}

### Change owner

To change the owner of a view:

{{% include-syntax file="examples/alter_view" example="syntax-change-owner" %}}

{{< /tab >}}

{{< /tabs >}}

## Privileges

The privileges required to execute this statement are:

{{< include-md
file="shared-content/sql-command-privileges/alter-view.md" >}}
