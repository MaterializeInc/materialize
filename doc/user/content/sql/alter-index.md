---
title: "ALTER INDEX"
description: "`ALTER INDEX` changes the parameters of an index."
menu:
  main:
    parent: 'commands'
---

`ALTER INDEX` changes the parameters of an index.

## Syntax

{{< diagram "alter-index.svg" >}}

Field | Use
------|-----
_name_ | The identifier of the index you want to alter.

## Details

#### Tables

Note that when enabling indexes on tables, the first index you enable must be
the table's primary index, which was created at the same time as the table
itself. Only after enabling the primary index can you enable any secondary
indexes.

## See also

- [`SHOW INDEXES`](/sql/show-indexes)
- [`SHOW CREATE VIEW`](/sql/show-create-view)
- [`SHOW VIEWS`](/sql/show-views)
- [`SHOW SOURCES`](/sql/show-sources)
