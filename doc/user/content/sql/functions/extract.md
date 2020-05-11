---
title: "EXTRACT Function"
description: "Returns a specified time component from a time-based value"
menu:
  main:
    parent: 'sql-functions'
---

`EXTRACT` returns some time component from a time-based value, such as the year from a Timestamp.

## Signatures

{{< diagram "func-extract.html" >}}

Parameter | Type | Description
----------|------|------------
_val_ | [`date`](../../types/date), [`timestamp`](../../types/timestamp), [`timestamptz`](../../types/timestamptz) | The value from which you want to extract a component.

### Return value

`EXTRACT` returns a [`float`](../../types/float) value.

## Examples

```sql
SELECT EXTRACT(SECOND FROM TIMESTAMP '2006-01-02 15:04:05.06') AS sec_extr;
```
```nofmt
 sec_extr
----------
     5.06
```
