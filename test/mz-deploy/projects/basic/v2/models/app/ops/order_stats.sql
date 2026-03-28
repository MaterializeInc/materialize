CREATE VIEW order_stats AS
    SELECT order_count, COUNT(*) AS user_count
    FROM app.core.user_activity
    GROUP BY order_count;

CREATE INDEX order_stats_idx IN CLUSTER app ON order_stats (order_count);

EXECUTE UNIT TEST test_order_stats_wrong
FOR app.ops.order_stats
MOCK app.core.user_activity(user_id integer, name text, order_count bigint) AS (
    SELECT * FROM (VALUES (1, 'Alice', 3), (2, 'Bob', 1), (3, 'Carol', 3))
),
EXPECTED(order_count bigint, user_count bigint) AS (
    SELECT * FROM (VALUES (3, 2), (1, 99))
);
