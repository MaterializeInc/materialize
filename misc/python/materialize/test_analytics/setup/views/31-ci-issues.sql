-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

CREATE OR REPLACE MATERIALIZED VIEW mv_ci_issues
IN CLUSTER test_analytics AS
    SELECT a.*, title, ci_regexp, state
    FROM (
        SELECT
            issue,
            count(*) AS occurrences,
            min(build_date) AS first_occurrence,
            max(build_date) AS last_occurrence
        FROM v_build_annotation_error
        WHERE issue IS NOT NULL
        GROUP BY issue
    )
    AS a
    JOIN issue AS i ON a.issue = i.issue_id
;

ALTER MATERIALIZED VIEW mv_ci_issues OWNER TO qa;
GRANT SELECT ON mv_ci_issues TO "ci-failures";
