---
title: "CREATE SOURCE: Text or bytes from local file"
description: "Learn how to connect Materialize to a text file"
menu:
  main:
    parent: 'create-source'
aliases:
    - /sql/create-source/text
    - /sql/create-source/text-source
    - /sql/create-source/byte
    - /sql/create-source/bytes
    - /sql/create-source/file
    - /sql/create-source/files
---

{{% create-source/intro %}}
This document details how to connect Materialize to text- or byte–formatted
local files.
{{% /create-source/intro %}}

### Formatting text

If you have unstructured text files, such as server logs, you can impose a
structure on them by providing a regular expression in the **REGEX** formatting
option. This lets you generate multiple columns from arbitrary lines of text,
given the text has some consistent structure.

If you want all of the text to come in as a single column, you can simply choose
the **TEXT** formatting option.

## Syntax

{{< diagram "create-source-text.svg" >}}

{{% create-source/syntax-details connector="file" formats="regex text bytes" envelopes="append-only" %}}

## Examples

### Creating a source from a dynamic, unstructured file

In this example, we'll assume we have [`xxd`](https://linux.die.net/man/1/xxd)
creating hex dumps for some incoming files. Its output might look like this:

```nofmt
00000000: 7f45 4c46 0201 0100 0000 0000 0000 0000  .ELF............
00000010: 0300 3e00 0100 0000 105b 0000 0000 0000  ..>......[......
00000020: 4000 0000 0000 0000 7013 0200 0000 0000  @.......p.......
```

We'll create a source that takes in these entire lines and extracts the file
offset, as well as the decoded value.

```sql
CREATE SOURCE hex
FROM FILE '/xxd.log'
WITH (tail = true)
FORMAT REGEX '(?P<offset>[0-9a-f]{8}): (?:[0-9a-f]{4} ){8} (?P<decoded>.*)$';
```

This creates a source that...

- Is append-only.
- Has two columns: `offset` and `decoded`.
- Discards the second group, i.e. `(?:[0-9a-f]{4} ){8}`.
- Materialize dynamically checks for new entries.

Using the above example, the source would generate data that looks similar to
this:

```nofmt
 offset  |     decoded
---------+------------------
00000000 | .ELF............
00000010 | ..>......[......
00000020 | @.......p.......
```

## Related pages

- [`CREATE SOURCE`](../)
- [`CREATE VIEW`](../../create-view)
- [`SELECT`](../../select)
