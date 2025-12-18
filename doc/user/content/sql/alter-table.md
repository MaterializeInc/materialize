---
title: "ALTER TABLE"
description: "`ALTER TABLE` changes properties of a table."
menu:
  main:
    parent: 'commands'
---

Use `ALTER TABLE` to:

- Rename a table.
- Change owner of a table.
- Change retain history configuration for the table.

## Syntax

{{< tabs >}}
{{< tab "Rename" >}}

### Rename

To rename a table:

{{% include-syntax file="examples/alter_table" example="syntax-rename" %}}

{{< /tab >}}
{{< tab "Change owner" >}}

### Change owner

To change the owner of a table:

{{% include-syntax file="examples/alter_table" example="syntax-change-owner" %}}

{{< /tab >}}
{{< tab "(Re)Set retain history config" >}}

### (Re)Set retain history config

To set the retention history for a user-populated table:

{{% include-syntax file="examples/alter_table" example="syntax-set-retain-history" %}}

To reset the retention history to the default for a user-populated table:

{{% include-syntax file="examples/alter_table" example="syntax-reset-retain-history" %}}

{{< /tab >}}
{{< /tabs >}}

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/alter-table.md" >}}
