Upstream tables replicated into Materialize should not be truncated. If an
upstream table is truncated while replicated, the whole source becomes
inaccessible and will not produce any data until it is recreated. Instead of
truncating, you can use an unqualified `DELETE` to remove all rows from the
table:

```mzsql
DELETE FROM t;
```
