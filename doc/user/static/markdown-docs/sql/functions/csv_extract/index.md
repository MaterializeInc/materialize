# csv_extract function
Returns separated values from a column containing a CSV file formatted as a string.
`csv_extract` returns individual component columns from a column containing a CSV file formatted as a string.

## Signatures



```mzsql
csv_extract ( <num_csv_col>, <col_name> )

```

| Syntax element | Description |
| --- | --- |
| `<num_csv_col>` | An [`int`](/sql/types/integer/) value specifying the number of columns in the CSV string.  |
| `<col_name>` | A [`string`](/sql/types/text/) value containing the name of the column containing the CSV string.  |


Parameter | Type | Description
----------|------|------------
_num_csv_col_ | [`int`](../../types/integer/) | The number of columns in the CSV string.
_col_name_  | [`string`](../../types/text/)  | The name of the column containing the CSV string.

### Return value

`EXTRACT` returns [`string`](../../types/text/) columns.

## Example

Create a table where one column is in CSV format and insert some rows:

```mzsql
CREATE TABLE t (id int, data string);
INSERT INTO t
  VALUES (1, 'some,data'), (2, 'more,data'), (3, 'also,data');
```

Extract the component columns from the table column which is a CSV string, sorted by column `id`:

```mzsql
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
