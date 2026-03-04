CREATE MATERIALIZED VIEW account_activity IN CLUSTER storefront AS
SELECT
    c.customer_id,
    c.name,
    c.email,
    c.signup_date,
    c.lifetime_orders,
    c.lifetime_spend,
    c.last_order_date,
    CASE
        WHEN c.lifetime_spend > 1000 THEN 'vip'
        WHEN c.lifetime_spend > 200 THEN 'regular'
        ELSE 'new'
    END AS loyalty_tier
FROM ontology.public.customers c
