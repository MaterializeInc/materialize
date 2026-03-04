CREATE VIEW active_products AS
SELECT
    p.product_id,
    p.name AS product_name,
    p.category,
    p.price,
    p.cost
FROM raw.public.products p
WHERE p.is_active = true;

CREATE INDEX active_products_by_id IN CLUSTER ontology ON active_products (product_id);

EXECUTE UNIT TEST test_filters_inactive_products
FOR ontology.internal.active_products
MOCK raw.public.products(product_id INTEGER, name TEXT, category TEXT, price NUMERIC, cost NUMERIC, is_active BOOLEAN) AS (
  VALUES
    (1, 'Widget', 'Gadgets', 19.99::NUMERIC, 10.00::NUMERIC, true),
    (2, 'Discontinued Item', 'Gadgets', 9.99::NUMERIC, 5.00::NUMERIC, false)
),
EXPECTED(product_id INTEGER, product_name TEXT, category TEXT, price NUMERIC, cost NUMERIC) AS (
  VALUES
    (1, 'Widget', 'Gadgets', 19.99::NUMERIC, 10.00::NUMERIC)
)
