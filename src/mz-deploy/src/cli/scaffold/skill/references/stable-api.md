### Stable API pattern

When creating a stable API schema, use a two-schema layout:

1. **Internal schema** — views with indexes containing all transformation
   logic. Place these in a sibling schema (e.g., `internal`) on the same
   cluster.
2. **Stable schema** (`SET api = stable`) — thin materialized views that
   simply `SELECT` from the corresponding internal view. Each MV should
   have a `COMMENT ON` describing its contract. Never use `SELECT *` always
   list out all columns.

Example layout:

    models/mydb/internal/customers_cleaned.sql   # CREATE VIEW + CREATE INDEX
    models/mydb/internal/orders_enriched.sql      # CREATE VIEW + CREATE INDEX
    models/mydb/public.sql                        # SET api = stable
    models/mydb/public/customers.sql              # CREATE MATERIALIZED VIEW ... AS SELECT * FROM mydb.internal.customers_cleaned
    models/mydb/public/orders.sql                 # CREATE MATERIALIZED VIEW ... AS SELECT * FROM mydb.internal.orders_enriched

This keeps all logic and indexes in the internal schema while exposing a
clean, stable API surface. Because stable MVs use the replacement protocol,
downstream consumers are never disrupted when the internal logic changes.