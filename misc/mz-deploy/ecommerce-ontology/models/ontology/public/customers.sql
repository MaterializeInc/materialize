CREATE MATERIALIZED VIEW customers IN CLUSTER ontology AS
SELECT * FROM ontology.internal.active_customer_detail;

COMMENT ON MATERIALIZED VIEW customers IS
    'Core customer entity. Each row is an active customer (non-null email) with '
    'lifetime aggregates: total orders, total spend, and last order date. '
    'Source of truth for customer identity across all downstream consumers.'
