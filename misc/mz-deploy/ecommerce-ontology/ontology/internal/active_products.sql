CREATE VIEW active_products AS
SELECT
    p.product_id,
    p.name AS product_name,
    p.category,
    p.price,
    p.cost
FROM raw.public.products p
WHERE p.is_active = true;

CREATE INDEX active_products_by_id IN CLUSTER ontology ON active_products (product_id)
