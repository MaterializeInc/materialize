-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

CREATE OR REPLACE VIEW v_data_integrity (table_name, own_item_key, referenced_item_key, problem) AS
    -- violated references
    SELECT 'build_job', build_job_id, build_id, 'build job references missing build'
    FROM build_job
    WHERE build_id NOT IN (SELECT build_id FROM build)
    UNION
    SELECT 'feature_benchmark_result', build_job_id, build_job_id, 'feature benchmark result references missing build job'
    FROM feature_benchmark_result
    WHERE build_job_id NOT IN (SELECT build_job_id FROM build_job)
    UNION
    SELECT 'scalability_framework_result', build_job_id, build_job_id, 'scalability result references missing build job'
    FROM scalability_framework_result
    WHERE build_job_id NOT IN (SELECT build_job_id FROM build_job)
    UNION
    SELECT 'parallel_benchmark_result', build_job_id, build_job_id, 'parallel benchmark result references missing build job'
    FROM parallel_benchmark_result
    WHERE build_job_id NOT IN (SELECT build_job_id FROM build_job)
    UNION
    SELECT 'product_limits_result', build_job_id, build_job_id, 'product limits result references missing build job'
    FROM product_limits_result
    WHERE build_job_id NOT IN (SELECT build_job_id FROM build_job)
    UNION
    SELECT 'build_annotation', build_job_id, build_job_id, 'build annotation references missing build job'
    FROM build_annotation
    WHERE build_job_id NOT IN (SELECT build_job_id FROM build_job)
    UNION
    SELECT 'build_annotation_error', build_job_id, build_job_id, 'build annotation error references missing build annotation'
    FROM build_annotation_error
    WHERE build_job_id NOT IN (SELECT build_job_id FROM build_annotation)
    UNION
    SELECT 'feature_benchmark_discarded_result', build_job_id, scenario_name, 'discarded benchmark result without actual result'
    FROM feature_benchmark_discarded_result
    WHERE (build_job_id, scenario_name) NOT IN (SELECT build_job_id, scenario_name FROM feature_benchmark_result)
    -- duplicate entries
    UNION
    SELECT 'build', build_id, NULL, 'duplicate build'
    FROM build
    GROUP BY build_id
    HAVING count(*) > 1
    UNION
    SELECT 'build_job', build_job_id, NULL, 'duplicate build job'
    FROM build_job
    GROUP BY build_job_id
    HAVING count(*) > 1
    UNION
    SELECT 'build_annotation', build_job_id, NULL, 'duplicate annotation for build job'
    FROM build_annotation
    GROUP BY build_job_id
    HAVING count(*) > 1
    -- other
    UNION
    SELECT 'build_job', build_job_id, NULL, 'build job id is not unique'
    FROM build_job
    GROUP BY build_job_id
    HAVING count(distinct build_id) > 1
    UNION
    SELECT 'build_job', concat(build_step_key, ', shard ', shard_index), max(build_job_id), 'multiple build jobs as latest retry'
    FROM build_job
    GROUP BY build_id, build_step_key, shard_index
    HAVING sum(CASE WHEN is_latest_retry THEN 1 ELSE 0 END) > 1
    UNION
    SELECT 'build', build_id, NULL, 'build without build jobs'
    FROM build
    WHERE NOT EXISTS (SELECT 1 FROM build_job WHERE build_id = build.build_id)
    UNION
    SELECT 'build_annotation', build_job_id, NULL, 'build job with multiple annotation entries'
    FROM build_annotation
    GROUP BY build_job_id
    HAVING count(*) > 1
    UNION
    SELECT 'config', 'config', NULL, 'more than one config entry exists'
    FROM config
    HAVING count(*) > 1
    UNION
    SELECT 'issue', issue_id, NULL, 'more than one issue entry exists'
    FROM issue
    GROUP BY issue_id
    HAVING count(*) > 1
;

ALTER VIEW v_data_integrity OWNER TO qa;
