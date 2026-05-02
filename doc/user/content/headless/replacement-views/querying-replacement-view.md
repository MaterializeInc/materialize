---
headless: true
---
You can query a replacement materialized view to validate its results before
replacing. However, when queried, replacement materialized views are treated
like a [view](/sql/create-view), and the query results are re-computed as part
of the query execution. As such, queries against replacement materialized views
are slower and more computationally expensive than queries against regular
materialized views.
