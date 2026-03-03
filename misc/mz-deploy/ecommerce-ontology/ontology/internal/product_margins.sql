CREATE VIEW product_margins AS
SELECT
    p.product_id,
    p.name AS product_name,
    p.category,
    p.price,
    p.cost,
    p.price - p.cost AS margin
FROM raw.public.products p
WHERE p.is_active = true
