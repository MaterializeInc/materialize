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
LEFT JOIN raw.public.orders o ON c.customer_id = o.customer_id AND o.status <> 'cancelled'
GROUP BY c.customer_id, c.email, c.name, c.signup_date;

EXECUTE UNIT TEST test_customer_with_orders
FOR ontology.internal.active_customer_detail
MOCK ontology.internal.customers_cleaned(customer_id INTEGER, email TEXT, name TEXT, signup_date DATE) AS (
  VALUES
    (1, 'alice@ex.com', 'Alice Smith', '2024-01-15'::DATE)
),
MOCK raw.public.orders(order_id INTEGER, customer_id INTEGER, product_id INTEGER, order_date TIMESTAMP WITHOUT TIME ZONE, total_amount NUMERIC, status TEXT) AS (
  VALUES
    (1, 1, 1, '2025-06-01 10:00:00'::TIMESTAMP, 100::NUMERIC, 'completed'),
    (2, 1, 2, '2025-07-01 12:00:00'::TIMESTAMP, 200::NUMERIC, 'completed')
),
EXPECTED(customer_id INTEGER, email TEXT, name TEXT, signup_date DATE, last_order_date TIMESTAMP WITHOUT TIME ZONE, lifetime_orders BIGINT, lifetime_spend NUMERIC) AS (
  VALUES
    (1, 'alice@ex.com', 'Alice Smith', '2024-01-15'::DATE, '2025-07-01 12:00:00'::TIMESTAMP, 2::BIGINT, 300::NUMERIC)
);

EXECUTE UNIT TEST test_customer_without_orders
FOR ontology.internal.active_customer_detail
MOCK ontology.internal.customers_cleaned(customer_id INTEGER, email TEXT, name TEXT, signup_date DATE) AS (
  VALUES
    (1, 'bob@ex.com', 'Bob Jones', '2024-02-01'::DATE)
),
MOCK raw.public.orders(order_id INTEGER, customer_id INTEGER, product_id INTEGER, order_date TIMESTAMP WITHOUT TIME ZONE, total_amount NUMERIC, status TEXT) AS (
  SELECT * FROM (VALUES (NULL::INTEGER, NULL::INTEGER, NULL::INTEGER, NULL::TIMESTAMP, NULL::NUMERIC, NULL::TEXT)) LIMIT 0
),
EXPECTED(customer_id INTEGER, email TEXT, name TEXT, signup_date DATE, last_order_date TIMESTAMP WITHOUT TIME ZONE, lifetime_orders BIGINT, lifetime_spend NUMERIC) AS (
  VALUES
    (1, 'bob@ex.com', 'Bob Jones', '2024-02-01'::DATE, NULL::TIMESTAMP, 0::BIGINT, NULL::NUMERIC)
)
