-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License in the LICENSE file at the
-- root of this repository, or online at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

CREATE OR REPLACE VIEW v_data_integrity (table_name, own_item_key, referenced_item_key, problem) AS
    -- violated references
    SELECT 'build_job', build_job_id, build_id, 'build job references missing build'
    FROM build_job
    WHERE build_id NOT IN (SELECT build_id FROM build)
    UNION
    SELECT 'feature_benchmark_result', build_job_id, build_job_id, 'benchmark result references missing build job'
    FROM feature_benchmark_result
    WHERE build_job_id NOT IN (SELECT build_job_id FROM build_job)
    UNION
    SELECT 'scalability_framework_result', build_job_id, build_job_id, 'scalability result references missing build job'
    FROM scalability_framework_result
    WHERE build_job_id NOT IN (SELECT build_job_id FROM build_job)
    UNION
    SELECT 'build_annotation', build_job_id, build_job_id, 'build annotation references missing build job'
    FROM build_annotation
    WHERE build_job_id NOT IN (SELECT build_job_id FROM build_job)
    UNION
    SELECT 'build_annotation_error', build_job_id, build_job_id, 'build annotation error references missing build annotation'
    FROM build_annotation_error
    WHERE build_job_id NOT IN (SELECT build_job_id FROM build_annotation)
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
