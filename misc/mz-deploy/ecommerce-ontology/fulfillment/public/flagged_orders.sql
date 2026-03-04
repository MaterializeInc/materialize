CREATE MATERIALIZED VIEW flagged_orders IN CLUSTER fulfillment AS
SELECT
    o.order_id,
    o.customer_id,
    c.name AS customer_name,
    c.email AS customer_email,
    o.total_amount,
    o.order_date,
    c.lifetime_orders,
    o.refund_amount,
    CASE
        WHEN o.total_amount > 500 AND c.lifetime_orders <= 1 THEN 'high_value_new_customer'
        WHEN o.refund_amount > 0 THEN 'prior_refund'
    END AS flag_reason
FROM ontology.public.orders o
JOIN ontology.public.customers c ON o.customer_id = c.customer_id
WHERE (o.total_amount > 500 AND c.lifetime_orders <= 1)
   OR o.refund_amount > 0
