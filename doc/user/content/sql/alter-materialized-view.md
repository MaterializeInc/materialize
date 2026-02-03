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

{{< if-released "v26.10" >}}

- Replace a materialized view. (*Public preview*)

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
{{< if-released "v26.10" >}}
{{< tab "Replace materialized view" >}}

### Replace materialized view

{{< public-preview />}}

To replace an existing materialized view in-place with a replacement
materialized view:

{{% include-syntax file="examples/alter_materialized_view" example="syntax-apply-replacement" %}}

{{< /tab >}}
{{< /if-released >}}
{{< /tabs >}}

{{< if-released "v26.10" >}}

## Details

### Replacing a materialized view

{{< public-preview />}}

{{% include-headless "/headless/replacement-views/associated-commands-blurb/"
%}}

{{% include-from-yaml data="examples/alter_materialized_view" name="apply-replacement-command-details" %}}

See [Recommended checks before replacing a
view](/sql/alter-materialized-view/#recommended-checks-before-replacing-a-view).

#### Recommended checks before replacing a view

{{% include-from-yaml data="examples/alter_materialized_view"
name="prereq-recommendations" %}}

{{< /if-released >}}

## Privileges

The privileges required to execute this statement are:

{{% include-headless "/headless/sql-command-privileges/alter-materialized-view"
%}}

{{< if-released "v26.10" >}}
## Examples

### Replace a materialized view

{{< public-preview />}}

{{% include-headless
"headless/replacement-views/replacement-view-target-restrictions" %}}

#### Prerequisite

{{% include-example file="examples/create_materialized_view"
example="example-create-replacement-materialized-view" %}}

The replacement view hydrates in the background.

#### Apply the replacement

{{< tip >}}

{{% include-from-yaml data="examples/alter_materialized_view"
name="prereq-recommendations-short" %}}

See [Recommended checks for
details](/sql/alter-materialized-view/#recommended-checks-before-replacing-a-view).

{{< /tip >}}

{{% include-example file="examples/alter_materialized_view"
example="example-apply-replacement" %}}

For a step-by-step tutorial on replacing a materialized view, see [Replace
materialized views
guide](/transform-data/updating-materialized-views/replace-materialized-view/).

{{< /if-released >}}

## Related pages

- [`CREATE MATERIALIZED VIEW`](/sql/create-materialized-view)
- [`SHOW MATERIALIZED VIEWS`](/sql/show-materialized-views)
- [`SHOW CREATE MATERIALIZED VIEW`](/sql/show-create-materialized-view)
- [`DROP MATERIALIZED VIEW`](/sql/drop-materialized-view)
