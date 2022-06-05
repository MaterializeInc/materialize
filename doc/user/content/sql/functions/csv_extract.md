---
title: "csv_extract function"
description: "Returns separated values from a column containing a CSV file formatted as a string."
menu:
  main:
    parent: 'sql-functions'
---

`csv_extract` returns individual component columns from a column containing a CSV file formatted as a string.

## Signatures

{{< diagram "func-csv-extract.svg" >}}

Parameter | Type | Description
----------|------|------------
_num_csv_col_ | [`int`](../../types/integer/) | The number of columns in the CSV string.
_col_name_  | [`string`](../../types/text/)  | The name of the column containing the CSV string.

### Return value

`EXTRACT` returns [`string`](../../types/text/) columns.

## Example

Create a table where one column is in CSV format and insert some rows:

```sql
CREATE TABLE t (id int, data string);
INSERT INTO t
  VALUES (1, 'some,data'), (2, 'more,data'), (3, 'also,data');
```

Extract the component columns from the table column which is a CSV string, sorted by column `id`:

```sql
SELECT csv.* FROM t, csv_extract(2, data) csv
  ORDER BY t.id;
```
```nofmt
 column1 | column2
---------+---------
 also    | data
 more    | data
 some    | data
(3 rows)
```
