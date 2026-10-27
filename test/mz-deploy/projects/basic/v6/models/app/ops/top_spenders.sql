-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

CREATE VIEW top_spenders AS
    SELECT user_id, name, total_spent
    FROM app.core.order_summary
    WHERE total_spent > 100;

CREATE INDEX top_spenders_idx IN CLUSTER app ON top_spenders (user_id);

EXECUTE UNIT TEST test_top_spenders_filter
FOR app.ops.top_spenders
MOCK app.core.order_summary(user_id integer, name text, order_count bigint, total_spent numeric) AS (
    SELECT * FROM (VALUES (1, 'Alice', 2, 150.00), (2, 'Bob', 1, 0.00))
)
EXPECTED(user_id integer, name text, total_spent numeric) AS (
    SELECT * FROM (VALUES (1, 'Alice', 150.00))
);
