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
FROM ontology.public.customers c;

EXECUTE UNIT TEST test_loyalty_tiers
FOR storefront.public.account_activity
MOCK ontology.public.customers(customer_id INTEGER, email TEXT, name TEXT, signup_date DATE, last_order_date TIMESTAMP WITHOUT TIME ZONE, lifetime_orders BIGINT, lifetime_spend NUMERIC) AS (
  VALUES
    (1, 'vip@ex.com', 'Vip User', '2024-01-01'::DATE, '2025-06-01 10:00:00'::TIMESTAMP, 10::BIGINT, 1500::NUMERIC),
    (2, 'reg@ex.com', 'Regular User', '2024-02-01'::DATE, '2025-07-01 10:00:00'::TIMESTAMP, 5::BIGINT, 500::NUMERIC),
    (3, 'new@ex.com', 'New User', '2024-03-01'::DATE, '2025-08-01 10:00:00'::TIMESTAMP, 1::BIGINT, 100::NUMERIC)
),
EXPECTED(customer_id INTEGER, name TEXT, email TEXT, signup_date DATE, lifetime_orders BIGINT, lifetime_spend NUMERIC, last_order_date TIMESTAMP WITHOUT TIME ZONE, loyalty_tier TEXT) AS (
  VALUES
    (1, 'Vip User', 'vip@ex.com', '2024-01-01'::DATE, 10::BIGINT, 1500::NUMERIC, '2025-06-01 10:00:00'::TIMESTAMP, 'vip'),
    (2, 'Regular User', 'reg@ex.com', '2024-02-01'::DATE, 5::BIGINT, 500::NUMERIC, '2025-07-01 10:00:00'::TIMESTAMP, 'regular'),
    (3, 'New User', 'new@ex.com', '2024-03-01'::DATE, 1::BIGINT, 100::NUMERIC, '2025-08-01 10:00:00'::TIMESTAMP, 'new')
)
