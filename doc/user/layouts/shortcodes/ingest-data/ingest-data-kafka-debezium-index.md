In Materialize, [indexes](/fundamentals/concepts/indexes) on views compute and, as new data
arrives, incrementally update view results in memory within a
[cluster](/fundamentals/concepts/clusters/) instead of recomputing the results from scratch.

Create an index on `cnt_table1` view. Then, as new change events stream in
through Kafka (as the result of `INSERT`, `UPDATE` and `DELETE` operations in
the upstream database), the index incrementally updates the view
results in memory, such that the in-memory up-to-date results are immediately
available and computationally free to query.

```mzsql
CREATE INDEX idx_cnt_table1_field1 ON cnt_table1(field1);
```

For best practices on when to index a view, see
[Indexes](/fundamentals/concepts/indexes/) and [Views](/fundamentals/concepts/views/).
