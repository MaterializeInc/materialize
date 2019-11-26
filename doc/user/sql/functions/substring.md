---
title: "SUBSTRING Function"
description: "Returns substring at specified positions"
menu:
  main:
    parent: 'sql-functions'
---

`SUBSTRING` returns a specified substring of a string.

## Parameters

{{< diagram "func-substring.html" >}}

Parameter | Type | Description
----------|------|------------
_str_ | String | The base string.
_start&lowbar;pos_ | Int | The starting position for the substring; counting starts at 1.
_len_ | Int | The length of the substring you want to return.

## Return value

`substring` returns a String.

## Examples

```sql
SELECT SUBSTRING('abcdefg', 3) AS substr;
```
```bash
 substr
--------
 cdefg
```

 <hr/>

```sql
SELECT SUBSTRING('abcdefg', 3, 3) AS substr;
```
```bash
 substr
--------
 cde
```
