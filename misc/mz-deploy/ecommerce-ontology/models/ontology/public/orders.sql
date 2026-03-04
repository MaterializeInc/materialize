CREATE MATERIALIZED VIEW orders IN CLUSTER ontology AS
SELECT * FROM ontology.internal.orders_enriched;

COMMENT ON MATERIALIZED VIEW orders IS
    'Core order entity. Each row is an order enriched with refund data: '
    'includes original total_amount, refund_amount (0 if none), and net_amount. '
    'Covers all order statuses (pending, shipped, completed).'
