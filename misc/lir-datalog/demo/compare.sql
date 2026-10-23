-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

-- The two views agree if both differences are empty.
SELECT
    (SELECT count(*) FROM flagged_transfers) AS sql_rows,
    (SELECT count(*) FROM flagged_transfers_fast) AS plan_rows,
    (SELECT count(*) FROM (
        SELECT * FROM flagged_transfers EXCEPT ALL SELECT * FROM flagged_transfers_fast
    )) AS only_in_sql,
    (SELECT count(*) FROM (
        SELECT * FROM flagged_transfers_fast EXCEPT ALL SELECT * FROM flagged_transfers
    )) AS only_in_plan;
