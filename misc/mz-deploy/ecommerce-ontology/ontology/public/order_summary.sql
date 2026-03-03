CREATE MATERIALIZED VIEW order_summary IN CLUSTER ontology AS
SELECT * FROM ontology.internal.orders_enriched
