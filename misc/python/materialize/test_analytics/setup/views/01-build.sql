-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

CREATE OR REPLACE VIEW v_most_recent_build AS
SELECT
    b.branch,
    b.pipeline,
    max(b.build_number) AS highest_build_number
FROM build b
GROUP BY
    b.branch,
    b.pipeline
;

CREATE OR REPLACE VIEW v_branch_type AS
WITH data AS
(
    SELECT
        b.branch,
        (b.branch = 'main') AS is_main_branch,
        (b.branch LIKE 'v%.%.%') AS is_release_branch,
        (b.branch <> 'main' AND b.branch NOT LIKE 'v%.%.%') AS is_feature_branch
    FROM build b
    GROUP BY
        b.branch
)
SELECT
    branch,
    CASE WHEN is_main_branch THEN 'main' WHEN is_release_branch THEN 'release' WHEN is_feature_branch THEN 'feature' ELSE '?' END AS branch_type,
    is_main_branch,
    is_release_branch,
    is_feature_branch
FROM data
;

ALTER VIEW v_most_recent_build OWNER TO qa;
ALTER VIEW v_branch_type OWNER TO qa;
