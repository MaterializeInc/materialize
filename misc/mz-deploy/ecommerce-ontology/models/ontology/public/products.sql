CREATE MATERIALIZED VIEW products IN CLUSTER ontology AS
SELECT * FROM ontology.internal.active_products;

COMMENT ON MATERIALIZED VIEW products IS
    'Core product entity. Each row is an active product with pricing and '
    'category information. Inactive products are excluded. '
    'Used by storefront for display and fulfillment for order context.'
