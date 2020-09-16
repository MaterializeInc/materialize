---
title: "ALTER INDEX"
description: "`ALTER INDEX` changes the parameters of an index."
menu:
  main:
    parent: 'sql'
---

{{< version-added v0.4.3 >}}

`ALTER INDEX` changes the parameters of an index.

## Syntax

{{< diagram "alter-index.svg" >}}

Field | Use
------|-----
_name_ | The identifier of the index you want to alter.
_field_ | The name of the parameter you want to alter.
_val_ | The new value for the parameter.

## Details

### Available parameters

Name                        | Meaning
----------------------------|--------
`logical_compaction_window` | Overrides the [logical compaction window](/ops/deployment#compaction) for the data stored in this index. The default value is controlled by the [`--logical-compaction-window`](/cli/#compaction-window) command-line option.

## Examples

To adjust the logical compaction window for the index named `some_primary_idx`:

```sql
ALTER INDEX some_primary_idx SET (logical_compaction_window = '500ms')
```

To reset the logical compaction window to its default value:

```sql
ALTER INDEX some_primary_idx RESET (logical_compaction_window)
```

## See also

- [`SHOW CREATE VIEW`](/sql/show-create-view)
- [`SHOW VIEWS`](/sql/show-views)
- [`SHOW SOURCES`](/sql/show-sources)
- [`SHOW INDEXES`](/sql/show-indexes)
- [`SHOW SINKS`](/sql/show-sinks)
