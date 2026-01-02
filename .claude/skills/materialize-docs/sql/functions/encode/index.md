---
audience: developer
canonical_url: https://materialize.com/docs/sql/functions/encode/
complexity: intermediate
description: Converts binary data to and from textual representations
doc_type: reference
keywords:
- SELECT DECODE
- encode and decode functions
- SELECT ENCODE
product_area: Indexes
status: stable
title: encode and decode functions
---

# encode and decode functions

## Purpose
Converts binary data to and from textual representations

If you need to understand the syntax and options for this command, you're in the right place.


Converts binary data to and from textual representations



The `encode` function encodes binary data into one of several textual
representations. The `decode` function does the reverse.

## Signatures

This section covers signatures.

```text
encode(b: bytea, format: text) -> text
decode(s: text, format: text) -> bytea
```

## Details

This section covers details.

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

```mzsql
SELECT encode('\x00404142ff', 'base64');
```text
```nofmt
  encode
----------
 AEBBQv8=
```text

```mzsql
SELECT decode('A   EB BQv8 =', 'base64');
```text
```nofmt
    decode
--------------
 \x00404142ff
```text

```mzsql
SELECT encode('This message is long enough that the output will run to multiple lines.', 'base64');
```text
```nofmt
                                    encode
------------------------------------------------------------------------------
 VGhpcyBtZXNzYWdlIGlzIGxvbmcgZW5vdWdoIHRoYXQgdGhlIG91dHB1dCB3aWxsIHJ1biB0byBt+
 dWx0aXBsZSBsaW5lcy4=
```text

<hr>

Encoding and decoding in the `escape` format:

```mzsql
SELECT encode('\x00404142ff', 'escape');
```text
```nofmt
   encode
-------------
 \000@AB\377
```text

```mzsql
SELECT decode('\000@AB\377', 'escape');
```text
```nofmt
    decode
--------------
 \x00404142ff
```text

<hr>

Encoding and decoding in the `hex` format:

```mzsql
SELECT encode('\x00404142ff', 'hex');
```text
```nofmt
   encode
------------
 00404142ff
```text

```mzsql
SELECT decode('00  40  41  42  ff', 'hex');
```text
```nofmt
    decode
--------------
 \x00404142ff
```

[rfc2045]: https://tools.ietf.org/html/rfc2045#section-6.8

