---
title: "EXTRACT Function"
description: "Returns a specified time component from a time-based value"
menu:
  main:
    parent: 'sql-functions'
---

`EXTRACT` returns some time component from a time-based value, such as the year from a Timestamp.

## Parameters

### func_extract

{{< diagram "func-extract.html" >}}

### time_component

{{< diagram "time-component.html" >}}

Parameter | Type | Description
----------|------|------------
_val_ | Time | The value from which you want to extract a component.

## Return value

`extract` returns a Float value.

## Examples

```sql
SELECT EXTRACT(SECOND FROM TIMESTAMP '2006-01-02 15:04:05.06') AS sec_extr;
```
```bash
 sec_extr
----------
     5.06
```
