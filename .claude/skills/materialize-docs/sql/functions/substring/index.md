---
audience: developer
canonical_url: https://materialize.com/docs/sql/functions/substring/
complexity: intermediate
description: Returns substring at specified positions
doc_type: reference
keywords:
- SELECT SUBSTRING
- SUBSTRING function
product_area: Indexes
status: stable
title: SUBSTRING function
---

# SUBSTRING function

## Purpose
Returns substring at specified positions

If you need to understand the syntax and options for this command, you're in the right place.


Returns substring at specified positions



`SUBSTRING` returns a specified substring of a string.

## Signatures

This section covers signatures.

```mzsql
substring(str, start_pos)
substring(str, start_pos, len)
substring(str FROM start_pos)
substring(str FOR len)
substring(str FROM start_pos FOR len)
```text

Parameter | Type | Description
----------|------|------------
_str_ | [`string`](../../types/string) | The base string.
_start&lowbar;pos_ | [`int`](../../types/int) | The starting position for the substring; counting starts at 1.
_len_ | [`int`](../../types/int) | The length of the substring you want to return.

### Return value

`substring` returns a [`string`](../../types/string).

## Examples

This section covers examples.

```mzsql
SELECT substring('abcdefg', 3) AS substr;
```text
```nofmt
 substr
--------
 cdefg
```text

 <hr/>

```mzsql
SELECT substring('abcdefg', 3, 3) AS substr;
```text
```nofmt
 substr
--------
 cde
```

