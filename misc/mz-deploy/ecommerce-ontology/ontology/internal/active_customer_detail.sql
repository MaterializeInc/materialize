CREATE VIEW active_customer_detail AS
SELECT
    c.customer_id,
    c.email,
    c.name,
    c.signup_date,
    MAX(o.order_date) AS last_order_date,
    COUNT(o.order_id) AS lifetime_orders,
    SUM(o.total_amount) AS lifetime_spend
FROM ontology.internal.customers_cleaned c
JOIN raw.public.orders o ON c.customer_id = o.customer_id
WHERE o.status <> 'cancelled'
GROUP BY c.customer_id, c.email, c.name, c.signup_date
