CREATE MATERIALIZED VIEW revenue_daily IN CLUSTER finance AS
SELECT
    os.order_date::date AS revenue_date,
    COUNT(*) AS order_count,
    SUM(os.net_amount) AS net_revenue,
    SUM(os.refund_amount) AS total_refunds
FROM ontology.public.order_summary os
GROUP BY os.order_date::date
