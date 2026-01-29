---
headless: true
---
You can use [`CREATE REPLACEMENT MATERIALIZED
VIEW`](/sql/create-materialized-view/) with [`ALTER MATERIALIZED VIEW ... APPLY
REPLACEMENT`](/sql/alter-materialized-view) to replace materialized views
in-place without downtime. That is, for a materialized view, you create a
replacement materialized view (which starts hydrating immediately), wait until
it hydrates, and then apply the replacement. This allows you to:
- **Preserve downstream objects**: Dependent views, materialized views,
  indexes, and sinks remain intact and do not need to be recreated.
- **Avoid downtime**: The replacement materialized view hydrates in the
  background while the original continues computing results.
- **Validate before applying**: You can verify that the replacement view is
  hydrated and produces the expected results before applying the replacement.
  See [Query
  performance](/sql/create-materialized-view/#query-performance-of-replacement-views)
  for details.

For a detailed guide on using [`CREATE REPLACEMENT MATERIALIZED
VIEW`](/sql/create-materialized-view/) with [`ALTER MATERIALIZED VIEW ... APPLY
REPLACEMENT`](/sql/alter-materialized-view),  see [Replace materialized
views](/transform-data/updating-materialized-views/replace-materialized-view/) guide.
