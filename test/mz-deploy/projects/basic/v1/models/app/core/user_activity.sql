CREATE MATERIALIZED VIEW user_activity
    IN CLUSTER compute
    AS
    SELECT u.user_id, u.name, COUNT(o.order_id) AS order_count
    FROM app.ingest.users u
    JOIN app.ingest.orders o ON u.user_id = o.user_id
    GROUP BY u.user_id, u.name;

EXECUTE UNIT TEST test_user_activity_counts
FOR app.core.user_activity
MOCK app.ingest.users(user_id integer, name text, email text) AS (
    SELECT * FROM (VALUES (1, 'Alice', 'a@test.com'))
),
MOCK app.ingest.orders(order_id integer, user_id integer, amount numeric, status text) AS (
    SELECT * FROM (VALUES (1, 1, 50.00, 'completed'), (2, 1, 75.00, 'pending'))
),
EXPECTED(user_id integer, name text, order_count bigint) AS (
    SELECT * FROM (VALUES (1, 'Alice', 2))
);

EXECUTE UNIT TEST test_user_activity_wrong_expectation
FOR app.core.user_activity
MOCK app.ingest.users(user_id integer, name text, email text) AS (
    SELECT * FROM (VALUES (1, 'Alice', 'a@test.com'))
),
MOCK app.ingest.orders(order_id integer, user_id integer, amount numeric, status text) AS (
    SELECT * FROM (VALUES (1, 1, 50.00, 'completed'))
),
EXPECTED(user_id integer, name text, order_count bigint) AS (
    SELECT * FROM (VALUES (1, 'Alice', 999))
);
