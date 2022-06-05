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
_field_ | The name of the option you want to alter.
_val_ | The new value for the option.

### `SET`/`RESET` options

The following option is valid within the `SET` and `RESET` clauses:

{{% index-with-options %}}

## Details

#### Tables

Note that when enabling indexes on tables, the first index you enable must be
the table's primary index, which was created at the same time as the table
itself. Only after enabling the primary index can you enable any secondary
indexes.

## Examples

To adjust the logical compaction window for the index named `some_primary_idx`:

```sql
ALTER INDEX some_primary_idx SET (logical_compaction_window = '500ms');
```

To reset the logical compaction window to its default value:

```sql
ALTER INDEX some_primary_idx RESET (logical_compaction_window);
```

## See also

- [`SHOW INDEXES`](/sql/show-indexes)
- [`SHOW CREATE VIEW`](/sql/show-create-view)
- [`SHOW VIEWS`](/sql/show-views)
- [`SHOW SOURCES`](/sql/show-sources)
- [`SHOW SINKS`](/sql/show-sinks)
