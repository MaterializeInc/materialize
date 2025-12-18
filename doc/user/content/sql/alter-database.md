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

{{% include-syntax file="examples/alter_database" example="syntax-rename" %}}

{{< /tab >}}
{{< tab "Change owner" >}}

### Change owner

To change the owner of a database:

{{% include-syntax file="examples/alter_database" example="syntax-change-owner" %}}

{{< /tab >}}

{{< /tabs >}}

## Privileges

The privileges required to execute this statement are:

{{< include-md
file="shared-content/sql-command-privileges/alter-database.md" >}}
