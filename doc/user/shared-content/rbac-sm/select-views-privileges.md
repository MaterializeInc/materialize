- `SELECT` privileges are required only on the directly referenced
  view/materialized view. `SELECT` privileges are **not** required for the
  underlying relations referenced in the view/materialized view definition
  unless those relations themselves are directly referenced in the query.

- However, the owner of the view/materialized view (including those with
  **superuser** privileges) must have all required `SELECT` and `USAGE`
  privileges to run the view definition regardless of who is selecting from the
  view/materialized view.
