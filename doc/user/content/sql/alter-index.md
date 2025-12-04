---
title: "ALTER INDEX"
description: "`ALTER INDEX` changes the parameters of an index."
menu:
  main:
    parent: 'commands'
---

Use `ALTER INDEX` to:
- Rename an index.

## Syntax

{{< tabs >}}
{{< tab "Rename" >}}

### Rename

To rename an index:

{{% include-syntax file="examples/alter_index" example="syntax-rename" %}}

{{< /tab >}}

{{< /tabs >}}


## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/alter-index.md" >}}

## Related pages

- [`SHOW INDEXES`](/sql/show-indexes)
- [`SHOW CREATE VIEW`](/sql/show-create-view)
- [`SHOW VIEWS`](/sql/show-views)
- [`SHOW SOURCES`](/sql/show-sources)
- [`SHOW SINKS`](/sql/show-sinks)
