-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

CREATE OR REPLACE MATERIALIZED VIEW mv_ci_failures
IN CLUSTER test_analytics AS
    SELECT
        pipeline || '#' || build_number AS build_identifier,
        test_suite,
        CASE WHEN error_type = 'KNOWN_ISSUE' THEN issue || ' (KNOWN ISSUE)' ELSE
          CASE WHEN error_type = 'POTENTIAL_REGRESSION' THEN issue || ' (POTENTIAL REGRESSION)' ELSE
          REPLACE(error_type, '_', ' ') END END AS issue,
        string_agg(content, '\n') AS content,
        branch,
        build_date,
        mz_version,
        commit_hash,
        build_step_key,
        test_retry_count,
        occurrence_count,
        build_id,
        build_job_id
    FROM v_build_annotation_error
    GROUP BY
        pipeline,
        build_number,
        test_suite,
        error_type,
        issue,
        branch,
        build_date,
        mz_version,
        commit_hash,
        build_step_key,
        test_retry_count,
        occurrence_count,
        build_id,
        build_job_id
;

CREATE INDEX IN CLUSTER test_analytics ON mv_ci_failures (branch, build_date, issue);

ALTER MATERIALIZED VIEW mv_ci_failures OWNER TO qa;
GRANT SELECT ON mv_ci_failures TO "ci-failures";
