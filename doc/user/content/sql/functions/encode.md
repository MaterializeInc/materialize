---
title: "encode and decode functions"
description: "Converts binary data to and from textual representations"
menu:
  main:
    parent: 'sql-functions'
---

The `encode` function encodes binary data into one of several textual
representations. The `decode` function does the reverse.

## Signatures

```
encode(b: bytea, format: text) -> text
decode(s: text, format: text) -> bytea
```

## Details

### Supported formats

The following formats are supported by both `encode` and `decode`.

#### `base64`

The `base64` format is defined in [Section 6.8 of RFC 2045][rfc2045].

To comply with the RFC, the `encode` function inserts a newline (`\n`) after
every 76 characters. The `decode` function ignores any whitespace in its input.

#### `escape`

The `escape` format renders zero bytes and bytes with the high bit set (`0x80` -
`0xff`) as an octal escape sequence (`\nnn`), renders backslashes as `\\`, and
renders all other characters literally. The `decode` function rejects invalid
escape sequences (e.g., `\9` or `\a`).

#### `hex`

The `hex` format represents each byte of input as two hexadecimal digits, with
the most significant digit first. The `encode` function uses lowercase for the
`a`-`f` digits, with no whitespace between digits. The `decode` function accepts
lowercase or uppercase for the `a` - `f` digits and permits whitespace between
each encoded byte, though not within a byte.

## Examples

Encoding and decoding in the `base64` format:

```sql
SELECT encode('\x00404142ff', 'base64');
```
```nofmt
  encode
----------
 AEBBQv8=
```

```sql
SELECT decode('A   EB BQv8 =', 'base64');
```
```nofmt
    decode
--------------
 \x00404142ff
```

```sql
SELECT encode('This message is long enough that the output will run to multiple lines.', 'base64');
```
```nofmt
                                    encode
------------------------------------------------------------------------------
 VGhpcyBtZXNzYWdlIGlzIGxvbmcgZW5vdWdoIHRoYXQgdGhlIG91dHB1dCB3aWxsIHJ1biB0byBt+
 dWx0aXBsZSBsaW5lcy4=
```

<hr>

Encoding and decoding in the `escape` format:

```sql
SELECT encode('\x00404142ff', 'escape');
```
```nofmt
   encode
-------------
 \000@AB\377
```

```sql
SELECT decode('\000@AB\377', 'escape');
```
```nofmt
    decode
--------------
 \x00404142ff
```

<hr>

Encoding and decoding in the `hex` format:

```sql
SELECT encode('\x00404142ff', 'hex');
```
```nofmt
   encode
------------
 00404142ff
```

```sql
SELECT decode('00  40  41  42  ff', 'hex');
```
```nofmt
    decode
--------------
 \x00404142ff
```

[rfc2045]: https://tools.ietf.org/html/rfc2045#section-6.8
