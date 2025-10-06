---
title: "normalize function"
description: "Returns a string normalized to the specified Unicode normalization form."
menu:
  main:
    parent: 'sql-functions'
---

`normalize` converts a string to a specified Unicode normalization form.

## Signatures

{{% include-example file="examples/normalize" example="syntax" %}}

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

{{% include-example file="examples/normalize" example="normalize-default" %}}

<hr/>

{{% include-example file="examples/normalize" example="normalize-nfc" %}}

<hr/>

{{% include-example file="examples/normalize" example="normalize-nfd" %}}

<hr/>

{{% include-example file="examples/normalize" example="normalize-nfkc-ligatures" %}}

<hr/>

{{% include-example file="examples/normalize" example="normalize-nfkc-superscript" %}}
