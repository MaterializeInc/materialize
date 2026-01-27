# FETCH
`FETCH` retrieves rows from a cursor.
`FETCH` retrieves rows from a query using a cursor previously opened with [`DECLARE`](/sql/declare).

## Syntax



```mzsql
FETCH [ <count> | ALL ] [FROM] <cursor_name> [ WITH ( TIMEOUT = <interval> ) ]

```

| Syntax element | Description |
| --- | --- |
| `<count>` | The number of rows to retrieve. Defaults to `1` if unspecified.  |
| `ALL` | Indicates that there is no limit on the number of rows to be returned.  |
| `<cursor_name>` | The name of an open cursor.  |
| `TIMEOUT` | When fetching from a [`SUBSCRIBE`](/sql/subscribe) cursor, complete if there are no more rows ready after this timeout. The default will cause `FETCH` to wait for at least one row to be available.  |


## Details

`FETCH` will return at most the specified _count_ of available rows. Specifying a _count_ of `ALL` indicates that there is no limit on the number of
rows to be returned.

For [`SUBSCRIBE`](/sql/subscribe) queries, `FETCH` by default will wait for rows to be available before returning.
Specifying a _timeout_ of `0s` returns only rows that are immediately available.
