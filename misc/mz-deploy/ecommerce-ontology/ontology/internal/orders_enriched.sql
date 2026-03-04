CREATE VIEW orders_enriched AS
SELECT
    o.order_id,
    o.customer_id,
    o.product_id,
    o.order_date,
    o.total_amount,
    o.status,
    COALESCE(r.refund_amount, 0) AS refund_amount,
    o.total_amount - COALESCE(r.refund_amount, 0) AS net_amount
FROM raw.public.orders o
LEFT JOIN raw.public.returns r ON o.order_id = r.order_id;

CREATE INDEX orders_enriched_by_customer IN CLUSTER ontology ON orders_enriched (customer_id);
CREATE INDEX orders_enriched_by_product IN CLUSTER ontology ON orders_enriched (product_id)
