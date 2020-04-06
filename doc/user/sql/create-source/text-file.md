---
title: "CREATE SOURCE: Text or bytes"
description: "Learn how to connect Materialize to a text file"
menu:
  main:
    parent: 'create-source'
aliases:
    - /docs/sql/create-source/text
    - /docs/sql/create-source/text-source
    - /docs/sql/create-source/byte
    - /docs/sql/create-source/bytes
    - /docs/sql/create-source/file
    - /docs/sql/create-source/files
---

`CREATE SOURCE` connects Materialize to some data source, and lets you interact
with its data as if it were in a SQL table.

This document details how to connect Materialize to locally available file. For
other options, view [`CREATE  SOURCE`](../).

## Conceptual framework

To provide data to Materialize, you must create "sources", which is a catchall
term for a resource Materialize can read data from. For more information, see
[API Components: Sources](../../../overview/api-components#sources).

### Formatting text

If you have unstructured text files, such as server logs, you can impose a
structure on them by providing a regular expression in the **REGEX** formatting
option. This lets you generate multiple columns from arbitrary lines of text,
given the text has some consistent structure.

If you want all of the text to come in as a single column, you can simply choose
the **TEXT** formatting option.

## Syntax

{{< diagram "create-source-text.html" >}}

Field | Use
------|-----
_src&lowbar;name_ | The name for the source, which is used as its table name within SQL.
_col&lowbar;name_ | Override default column name with the provided [identifier](../../identifiers). If used, a _col&lowbar;name_ must be provided for each column in the created source.
**FILE** _path_ | The absolute path to the file you want to use as the source.
**WITH (** _option&lowbar;list_ **)** | Options affecting source creation. For more detail, see [`WITH` options](#with-options).
**REGEX** _regex_ | Format the source's data as a string, applying _regex_, whose capture groups define the columns of the relation. For more detail, see [Regex format details](#regex-format-details).
**TEXT** | Format the source's data as ASCII-encoded text.
**BYTES** | Format the source's data as unformatted bytes.

#### `WITH` options

The following options are valid within the `WITH` clause.

Field | Value | Description
------|-------|------------
`tail` | `bool` | Continually check the file for new content; as new content arrives, process it using other `WITH` options.

## Details

This document assumes you're using a locally available file to load text or byte
data into Materialize.

### File source details

- `path` values must be the file's absolute path, e.g.
    ```sql
    CREATE SOURCE server_source FROM FILE '/Users/sean/server.log'...
    ```
- All data in file sources are treated as [`string`](../../types/string).

### Regex format details

Regex-formatted sources let you apply a structure to arbitrary strings passed in
from file sources. This is particularly useful when processing unstructured log
files.

- To parse regex strings, Materialize uses
  [rust-lang/regex](https://github.com/rust-lang/regex). For more detail, refer
  to its [documented syntax](https://docs.rs/regex/latest/regex/#syntax).
- To create a column in the source, create a capture group, i.e. a parenthesized
  expression, e.g. `([0-9a-f]{8})`.
    - Name columns by...
      - Using the _col&lowbar;name_ option when creating the source. The number
        of names provided must match the number of capture groups.
      - Creating named captured groups, e.g. `?P<offset>` in
        `(?P<offset>[0-9a-f]{8})` creates a column named `offset`.

        Unnamed capture groups are named `column1`, `column2`, etc.
- We discard all data not included in a capture group. You can create
  non-capturing groups using `?:` as the leading pattern in the group, e.g.
  `(?:[0-9a-f]{4} ){8}`.

### Text format details

Text-formatted sources reads lines from a file.

- Data from text-formatted sources is treated as newline-delimited.
- Data is assumed to be UTF-8 encoded, and discarded if it cannot be converted
  to UTF-8.
- Text-formatted sources have one column, which, by default, is named `text`.

### Raw byte format details

Raw byte-formatted sources provide Materialize the raw bytes received from the
source without applying any formatting or decoding.

Bext-formatted sources have one column, which, by default, is named `bytes`.

### Envelope details

Envelopes determine whether an incoming record inserts new data, updates or
deletes existing data, or both. For more information, see [API Components:
Envelopes](../../../overview/api-components#envelopes).

Text or byte data can currently only be handled as append-only by Materialize.

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
