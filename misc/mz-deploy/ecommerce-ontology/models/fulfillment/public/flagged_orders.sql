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
   OR o.refund_amount > 0;

EXECUTE UNIT TEST test_high_value_new_customer_flag
FOR fulfillment.public.flagged_orders
MOCK ontology.public.orders(order_id INTEGER, customer_id INTEGER, product_id INTEGER, order_date TIMESTAMP WITHOUT TIME ZONE, total_amount NUMERIC, status TEXT, refund_amount NUMERIC, net_amount NUMERIC) AS (
  VALUES
    (1, 1, 1, '2025-06-01 10:00:00'::TIMESTAMP, 600::NUMERIC, 'completed', 0::NUMERIC, 600::NUMERIC)
),
MOCK ontology.public.customers(customer_id INTEGER, email TEXT, name TEXT, signup_date DATE, last_order_date TIMESTAMP WITHOUT TIME ZONE, lifetime_orders BIGINT, lifetime_spend NUMERIC) AS (
  VALUES
    (1, 'alice@ex.com', 'Alice Smith', '2024-01-15'::DATE, '2025-06-01 10:00:00'::TIMESTAMP, 1::BIGINT, 600::NUMERIC)
),
EXPECTED(order_id INTEGER, customer_id INTEGER, customer_name TEXT, customer_email TEXT, total_amount NUMERIC, order_date TIMESTAMP WITHOUT TIME ZONE, lifetime_orders BIGINT, refund_amount NUMERIC, flag_reason TEXT) AS (
  VALUES
    (1, 1, 'Alice Smith', 'alice@ex.com', 600::NUMERIC, '2025-06-01 10:00:00'::TIMESTAMP, 1::BIGINT, 0::NUMERIC, 'high_value_new_customer')
);

EXECUTE UNIT TEST test_prior_refund_flag
FOR fulfillment.public.flagged_orders
MOCK ontology.public.orders(order_id INTEGER, customer_id INTEGER, product_id INTEGER, order_date TIMESTAMP WITHOUT TIME ZONE, total_amount NUMERIC, status TEXT, refund_amount NUMERIC, net_amount NUMERIC) AS (
  VALUES
    (2, 2, 2, '2025-07-01 10:00:00'::TIMESTAMP, 100::NUMERIC, 'completed', 20::NUMERIC, 80::NUMERIC)
),
MOCK ontology.public.customers(customer_id INTEGER, email TEXT, name TEXT, signup_date DATE, last_order_date TIMESTAMP WITHOUT TIME ZONE, lifetime_orders BIGINT, lifetime_spend NUMERIC) AS (
  VALUES
    (2, 'bob@ex.com', 'Bob Jones', '2024-02-01'::DATE, '2025-07-01 10:00:00'::TIMESTAMP, 5::BIGINT, 500::NUMERIC)
),
EXPECTED(order_id INTEGER, customer_id INTEGER, customer_name TEXT, customer_email TEXT, total_amount NUMERIC, order_date TIMESTAMP WITHOUT TIME ZONE, lifetime_orders BIGINT, refund_amount NUMERIC, flag_reason TEXT) AS (
  VALUES
    (2, 2, 'Bob Jones', 'bob@ex.com', 100::NUMERIC, '2025-07-01 10:00:00'::TIMESTAMP, 5::BIGINT, 20::NUMERIC, 'prior_refund')
)
