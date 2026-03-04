CREATE MATERIALIZED VIEW shipping_updates IN CLUSTER storefront AS
SELECT
    o.order_id,
    o.status,
    o.order_date,
    o.total_amount,
    c.customer_id,
    c.name AS customer_name,
    c.email AS customer_email,
    p.product_name
FROM ontology.public.orders o
JOIN ontology.public.customers c ON o.customer_id = c.customer_id
JOIN ontology.public.products p ON o.product_id = p.product_id
WHERE o.status IN ('pending', 'shipped', 'completed')
