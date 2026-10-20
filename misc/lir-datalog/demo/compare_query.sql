-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

-- The view agrees with the query of flagged_transfers.sql, run directly over the
-- tables, if both differences are empty.
WITH query AS (
    SELECT t.*
    FROM transfers t
    WHERE EXISTS (
        SELECT 1 FROM flagged f WHERE f.account = t.src OR f.account = t.dst
    )
)
SELECT
    (SELECT count(*) FROM query) AS query_rows,
    (SELECT count(*) FROM flagged_transfers) AS view_rows,
    (SELECT count(*) FROM (
        SELECT * FROM query EXCEPT ALL SELECT * FROM flagged_transfers
    )) AS only_in_query,
    (SELECT count(*) FROM (
        SELECT * FROM flagged_transfers EXCEPT ALL SELECT * FROM query
    )) AS only_in_view;
