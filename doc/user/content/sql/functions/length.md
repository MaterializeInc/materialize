---
title: "LENGTH function"
description: "Returns the number of characters in a string."
menu:
  main:
    parent: 'sql-functions'
---

`LENGTH` returns the [code points](https://en.wikipedia.org/wiki/Code_point) in
an encoded string.

## Signatures

{{< diagram "func-length.svg" >}}

Parameter | Type | Description
----------|------|------------
_str_ | [`string`](../../types/string) or `bytea` | The string whose length you want.
_encoding&lowbar;name_ | [`string`](../../types/string) | The [encoding](#encoding-details) you want to use for calculating the string's length. _Defaults to UTF-8_.

### Return value

`length` returns an [`int`](../../types/int).

## Details

### Errors

`length` operations might return `NULL` values indicating errors in the
following cases:

- The _encoding&lowbar;name_ provided is not available in our encoding package.
- Some byte sequence in _str_ was not compatible with the selected encoding.

### Encoding details

- Materialize uses the [`encoding`](https://crates.io/crates/encoding) crate.
  See the [list of supported
  encodings](https://lifthrasiir.github.io/rust-encoding/encoding/index.html#supported-encodings),
  as well as their names [within the
  API](https://github.com/lifthrasiir/rust-encoding/blob/4e79c35ab6a351881a86dbff565c4db0085cc113/src/label.rs).
- Materialize attempts to convert [PostgreSQL-style encoding
  names](https://www.postgresql.org/docs/9.5/multibyte.html) into the
  [WHATWG-style encoding names](https://encoding.spec.whatwg.org/) used by the
  API.

    For example, you can refer to `iso-8859-5` (WHATWG-style) as `ISO_8859_5`
    (PostrgreSQL-style).

    However, there are some differences in the names of the same encodings that
    we do not convert. For example, the
    [windows-874](https://encoding.spec.whatwg.org/#windows-1252) encoding is
    referred to as `WIN874` in PostgreSQL; Materialize does not convert these names.

## Examples

```mzsql
SELECT length('你好') AS len;
```
```nofmt
 len
-----
   2
```

<hr/>

```mzsql
SELECT length('你好', 'big5') AS len;
```
```nofmt
 len
-----
   3
```
