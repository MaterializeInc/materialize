A [view](/concepts/views/) saves a query under a name to provide a shorthand for
referencing the query. During view creation, the underlying query is not
executed.

```mzsql
CREATE VIEW cnt_table1 AS
    SELECT field1,
           COUNT(*) AS cnt
    FROM kafka_repl
    GROUP BY field1;
```
