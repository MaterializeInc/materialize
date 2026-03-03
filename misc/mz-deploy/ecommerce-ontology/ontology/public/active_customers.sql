CREATE MATERIALIZED VIEW active_customers IN CLUSTER ontology AS
SELECT * FROM ontology.internal.active_customer_detail
