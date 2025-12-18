---
title: "ALTER MATERIALIZED VIEW"
description: "`ALTER MATERIALIZED VIEW` changes the parameters of a materialized view."
menu:
  main:
    parent: 'commands'
---

Use `ALTER  MATERIALIZED VIEW` to:

- Rename a materialized view.
- Change owner of a materialized view.
- Change retain history configuration for the materialized view.

## Syntax

{{< tabs >}}
{{< tab "Rename" >}}

### Rename

To rename a materialized view:

{{% include-syntax file="examples/alter_materialized_view" example="syntax-rename" %}}

{{< /tab >}}
{{< tab "Change owner" >}}

### Change owner

To change the owner of a materialized view:

{{% include-syntax file="examples/alter_materialized_view" example="syntax-change-owner" %}}

{{< /tab >}}
{{< tab "(Re)Set retain history config" >}}

### (Re)Set retain history config

To set the retention history for a materialized view:

{{% include-syntax file="examples/alter_materialized_view" example="syntax-set-retain-history" %}}

To reset the retention history to the default for a materialized view:

{{% include-syntax file="examples/alter_materialized_view" example="syntax-reset-retain-history" %}}

{{< /tab >}}
{{< /tabs >}}

## Details

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/alter-materialized-view.md" >}}

## Related pages

- [`SHOW MATERIALIZED VIEWS`](/sql/show-materialized-views)
- [`SHOW CREATE MATERIALIZED VIEW`](/sql/show-create-materialized-view)
