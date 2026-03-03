CREATE MATERIALIZED VIEW customer_segments IN CLUSTER marketing AS
SELECT
    ac.customer_id,
    ac.email,
    ac.name,
    ac.lifetime_spend,
    ac.lifetime_orders,
    CASE
        WHEN ac.lifetime_spend > 1000 THEN 'vip'
        WHEN ac.lifetime_spend > 200 THEN 'regular'
        ELSE 'new'
    END AS segment
FROM ontology.public.active_customers ac
