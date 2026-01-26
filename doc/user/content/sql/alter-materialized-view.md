---
title: "ALTER MATERIALIZED VIEW"
description: "`ALTER MATERIALIZED VIEW` changes the parameters of a materialized view."
menu:
  main:
    parent: 'commands'
---

Use `ALTER MATERIALIZED VIEW` to:

- Rename a materialized view.
- Change owner of a materialized view.
- Change retain history configuration for the materialized view.

{{< if-released "v26.11" >}}

- Apply a replacement materialized view. *Public preview*.

{{< /if-released >}}

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
{{< if-released "v26.11" >}}
{{< tab "Apply replacement" >}}

### Apply replacement

{{< public-preview />}}

To apply a replacement materialized view:

{{% include-syntax file="examples/alter_materialized_view" example="syntax-apply-replacement" %}}

This operation replaces the definition of the target materialized view with the definition of the replacement, and drops the replacement at the same time.

{{< /tab >}}
{{< /if-released >}}
{{< /tabs >}}

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/alter-materialized-view.md" >}}

## Related pages

- [`CREATE MATERIALIZED VIEW`](/sql/create-materialized-view)
- [`SHOW MATERIALIZED VIEWS`](/sql/show-materialized-views)
- [`SHOW CREATE MATERIALIZED VIEW`](/sql/show-create-materialized-view)
- [`DROP MATERIALIZED VIEW`](/sql/drop-materialized-view)
