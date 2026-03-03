CREATE MATERIALIZED VIEW product_catalog IN CLUSTER ontology AS
SELECT * FROM ontology.internal.product_margins
