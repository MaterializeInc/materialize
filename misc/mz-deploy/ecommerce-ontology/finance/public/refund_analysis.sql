CREATE MATERIALIZED VIEW refund_analysis IN CLUSTER finance AS
SELECT
    pc.category,
    COUNT(os.order_id) AS total_orders,
    SUM(CASE WHEN os.refund_amount > 0 THEN 1 ELSE 0 END) AS refunded_orders,
    SUM(os.refund_amount) AS total_refund_amount
FROM ontology.public.order_summary os
JOIN ontology.public.product_catalog pc ON os.product_id = pc.product_id
GROUP BY pc.category
