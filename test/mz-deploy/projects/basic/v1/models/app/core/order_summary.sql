CREATE MATERIALIZED VIEW order_summary
    IN CLUSTER compute
    AS
    SELECT u.user_id, u.name, COUNT(o.order_id) AS order_count, SUM(o.amount) AS total_spent
    FROM app.ingest.users u
    JOIN app.ingest.orders o ON u.user_id = o.user_id
    GROUP BY u.user_id, u.name;

EXECUTE UNIT TEST test_order_aggregation
FOR app.core.order_summary
MOCK app.ingest.users(user_id integer, name text, email text) AS (
    SELECT * FROM (VALUES (1, 'Alice', 'alice@test.com'), (2, 'Bob', 'bob@test.com'))
),
MOCK app.ingest.orders(order_id integer, user_id integer, amount numeric, status text) AS (
    SELECT * FROM (VALUES (1, 1, 100.00, 'completed'), (2, 1, 50.00, 'completed'), (3, 2, 200.00, 'pending'))
),
EXPECTED(user_id integer, name text, order_count bigint, total_spent numeric) AS (
    SELECT * FROM (VALUES (1, 'Alice', 2, 150.00), (2, 'Bob', 1, 200.00))
);
