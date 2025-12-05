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

{{% include-syntax file="examples/alter_type" example="syntax-rename" %}}

{{< /tab >}}
{{< tab "Change owner" >}}

### Change owner

To change the owner of a type:

{{% include-syntax file="examples/alter_type" example="syntax-change-owner" %}}

{{< /tab >}}

{{< /tabs >}}

## Privileges

The privileges required to execute this statement are:

{{< include-md
file="shared-content/sql-command-privileges/alter-type.md" >}}
