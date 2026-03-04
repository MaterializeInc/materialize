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
CREATE INDEX orders_enriched_by_product IN CLUSTER ontology ON orders_enriched (product_id);

EXECUTE UNIT TEST test_order_with_refund
FOR ontology.internal.orders_enriched
MOCK raw.public.orders(order_id INTEGER, customer_id INTEGER, product_id INTEGER, order_date TIMESTAMP WITHOUT TIME ZONE, total_amount NUMERIC, status TEXT) AS (
  VALUES
    (1, 1, 1, '2025-06-01 10:00:00'::TIMESTAMP, 100::NUMERIC, 'completed')
),
MOCK raw.public.returns(return_id INTEGER, order_id INTEGER, refund_amount NUMERIC) AS (
  VALUES
    (1, 1, 25::NUMERIC)
),
EXPECTED(order_id INTEGER, customer_id INTEGER, product_id INTEGER, order_date TIMESTAMP WITHOUT TIME ZONE, total_amount NUMERIC, status TEXT, refund_amount NUMERIC, net_amount NUMERIC) AS (
  VALUES
    (1, 1, 1, '2025-06-01 10:00:00'::TIMESTAMP, 100::NUMERIC, 'completed', 25::NUMERIC, 75::NUMERIC)
);

EXECUTE UNIT TEST test_order_without_refund
FOR ontology.internal.orders_enriched
MOCK raw.public.orders(order_id INTEGER, customer_id INTEGER, product_id INTEGER, order_date TIMESTAMP WITHOUT TIME ZONE, total_amount NUMERIC, status TEXT) AS (
  VALUES
    (2, 2, 2, '2025-07-10 09:00:00'::TIMESTAMP, 200::NUMERIC, 'completed')
),
MOCK raw.public.returns(return_id INTEGER, order_id INTEGER, refund_amount NUMERIC) AS (
  SELECT * FROM (VALUES (NULL::INTEGER, NULL::INTEGER, NULL::NUMERIC)) LIMIT 0
),
EXPECTED(order_id INTEGER, customer_id INTEGER, product_id INTEGER, order_date TIMESTAMP WITHOUT TIME ZONE, total_amount NUMERIC, status TEXT, refund_amount NUMERIC, net_amount NUMERIC) AS (
  VALUES
    (2, 2, 2, '2025-07-10 09:00:00'::TIMESTAMP, 200::NUMERIC, 'completed', 0::NUMERIC, 200::NUMERIC)
)
