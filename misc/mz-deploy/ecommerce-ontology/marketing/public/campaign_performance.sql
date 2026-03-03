CREATE VIEW campaign_performance AS
SELECT
    cs.segment,
    COUNT(*) AS customer_count,
    SUM(cs.lifetime_spend) AS total_spend,
    AVG(cs.lifetime_orders)::numeric(10, 2) AS avg_orders
FROM marketing.public.customer_segments cs
GROUP BY cs.segment
