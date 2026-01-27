# normalize function
Returns a string normalized to the specified Unicode normalization form.
`normalize` converts a string to a specified Unicode normalization form.

## Signatures

<no value>```mzsql
normalize(str)
normalize(str, form)

```

Parameter | Type | Description
----------|------|------------
_str_ | [`string`](../../types/string) | The string to normalize.
_form_ | keyword | The Unicode normalization form: `NFC`, `NFD`, `NFKC`, or `NFKD` (unquoted, case-insensitive keywords). _Defaults to `NFC`_.

### Return value

`normalize` returns a [`string`](../../types/string).

## Details

Unicode normalization is a process that converts different binary representations of characters to a canonical form. This is useful when comparing strings that may have been encoded differently.

The four normalization forms are:

- **NFC** (Normalization Form Canonical Composition): Canonical decomposition, followed by canonical composition. This is the default and most commonly used form.
- **NFD** (Normalization Form Canonical Decomposition): Canonical decomposition only. Characters are decomposed into their constituent parts.
- **NFKC** (Normalization Form Compatibility Composition): Compatibility decomposition, followed by canonical composition. This applies more aggressive transformations, converting compatibility variants to standard forms.
- **NFKD** (Normalization Form Compatibility Decomposition): Compatibility decomposition only.

For more information, see:
- [Unicode Normalization Forms](https://unicode.org/reports/tr15/#Norm_Forms)
- [PostgreSQL normalize function](https://www.postgresql.org/docs/current/functions-string.html)

## Examples

Normalize a string using the default NFC form:
```mzsql
SELECT normalize('é') AS normalized;

``````nofmt
 normalized
------------
 é
```


<hr/>

NFC combines base character with combining marks:
```mzsql
SELECT normalize('é', NFC) AS nfc;

``````nofmt
 nfc
-----
 é
```


<hr/>

NFD decomposes into base character + combining accent:
```mzsql
SELECT normalize('é', NFD) = E'e\u0301' AS is_decomposed;

``````nofmt
 is_decomposed
---------------
 true
```


<hr/>

NFKC decomposes compatibility characters like ligatures:
```mzsql
SELECT normalize('ﬁ', NFKC) AS decomposed;

``````nofmt
 decomposed
------------
 fi
```


<hr/>

NFKC converts superscripts to regular characters:
```mzsql
SELECT normalize('x²', NFKC) AS normalized;

``````nofmt
 normalized
------------
 x2
```
