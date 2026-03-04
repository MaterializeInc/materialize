CREATE MATERIALIZED VIEW product_detail IN CLUSTER storefront AS
SELECT
    p.product_id,
    p.product_name,
    p.category,
    p.price,
    COUNT(o.order_id) AS units_sold
FROM ontology.public.products p
LEFT JOIN ontology.public.orders o ON p.product_id = o.product_id
GROUP BY p.product_id, p.product_name, p.category, p.price
