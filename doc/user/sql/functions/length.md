---
title: "length"
description: "Returns the number of characters in a string."
menu:
  main:
    parent: 'sql-functions'
---

The `length` function returns the number of characters in a provided string.

## Parameters

{{< diagram "func-length.html" >}}

Parameter | Type | Description
----------|------|------------
_str_ | String | The string whose length you want.
_encoding&lowbar;name_ | String | The [encoding](#encoding-details) you want to use for calculating the string's length. _Defaults to UTF-8_.

## Return value

`length` returns an Int.

## Details

### Errors

`length` operations might return `NULL` values indicating errors in the following cases:

- The _encoding&lowbar;name_ provided is not available in our encoding package.
- Some byte sequence in _str_ was not compatible with the provided encoding.

### Fixed-width strings

Materialize returns the length of fixed-width strings as the full width of the string. For example `length` on a `CHAR(15)` column returns `15` as each string's length.

Materialize recieves strings from your database in the same format they are emitted. In the case of fixed-width strings, e.g. `CHAR` columns in PostgreSQL, we receive the value padded by empty spaces. Because we cannot determine whether those spaces were intentional, or an artifact of a fixed-width string, we provide the length of the string as we received it.

You can find any updates on this behavior in [this GitHub issue](https://github.com/MaterializeInc/materialize/issues/589).

### Encoding details

- Materialize uses the [`encoding`](https://crates.io/crates/encoding) crate. See the [list of supported encodings](https://lifthrasiir.github.io/rust-encoding/encoding/index.html#supported-encodings), as well as their names [within the API](https://github.com/lifthrasiir/rust-encoding/blob/4e79c35ab6a351881a86dbff565c4db0085cc113/src/label.rs).
- Materialize attempts to convert [PostgreSQL-style encoding names](https://www.postgresql.org/docs/9.5/multibyte.html) into the [WHATWG-style encoding names](https://encoding.spec.whatwg.org/) used by the API. 
    
    For example, you can refer to `iso-8859-5` (WHATWG-style) as `ISO_8859_5` (PostrgreSQL-style).

## Examples

```sql
SELECT LENGTH('你好') AS len;
```
```bash
 len
-----
   2
```

<hr/>

```sql
SELECT LENGTH('你好', 'big5') AS len;
```
```bash
 len
-----
   3
```
