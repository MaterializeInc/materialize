---
title: "SHOW CREATE SOURCE"
description: "`SHOW CREATE SOURCE` returns the URL used to create the source."
menu:
  main:
    parent: commands
---

`SHOW CREATE SOURCE` returns the URL used to create the source.

## Syntax

{{< diagram "show-create-source.svg" >}}

Field | Use
------|-----
_source&lowbar;name_ | The source you want use. You can find available source names through [`SHOW SOURCES`](../show-sources).

## Details

You can determine a source's type by the **Source URL** address prefix.

Prefix | Type
-------|------
`kafka://` | Streaming
`file://` | File

## Examples

```sql
SHOW CREATE SOURCE my_source;
```
```nofmt
    Source   |        Source URL
-------------+--------------------------
 my_source   | file:///file-source.csv
```

## Related pages

- [`SHOW SOURCES`](../show-sources)
- [`CREATE SOURCE`](../create-source)
