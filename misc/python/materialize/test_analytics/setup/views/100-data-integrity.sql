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
    SELECT 'build_step', build_step_id, build_id, 'build step references missing build'
    FROM build_step
    WHERE build_id NOT IN (SELECT build_id FROM build)
    UNION
    SELECT 'feature_benchmark_result', build_step_id, build_step_id, 'benchmark result references missing build step'
    FROM feature_benchmark_result
    WHERE build_step_id NOT IN (SELECT build_step_id FROM build_step)
    UNION
    SELECT 'scalability_framework_result', build_step_id, build_step_id, 'scalability result references missing build step'
    FROM scalability_framework_result
    WHERE build_step_id NOT IN (SELECT build_step_id FROM build_step)
    UNION
    SELECT 'build_annotation', build_step_id, build_step_id, 'build annotation references missing build step'
    FROM build_annotation
    WHERE build_step_id NOT IN (SELECT build_step_id FROM build_step)
    UNION
    SELECT 'build_annotation_error', build_step_id, build_step_id, 'build annotation error references missing build annotation'
    FROM build_annotation_error
    WHERE build_step_id NOT IN (SELECT build_step_id FROM build_annotation)
    -- duplicate entries
    UNION
    SELECT 'build', build_id, NULL, 'duplicate build'
    FROM build
    GROUP BY build_id
    HAVING count(*) > 1
    UNION
    SELECT 'build_step', build_step_id, NULL, 'duplicate build step'
    FROM build_step
    GROUP BY build_step_id
    HAVING count(*) > 1
    UNION
    SELECT 'build_annotation', build_step_id, NULL, 'duplicate annotation for build step'
    FROM build_annotation
    GROUP BY build_step_id
    HAVING count(*) > 1
    -- other
    UNION
    SELECT 'build_step', build_step_id, NULL, 'build step id is not unique'
    FROM build_step
    GROUP BY build_step_id
    HAVING count(distinct build_id) > 1
    UNION
    SELECT 'build_step', build_step_key, max(build_step_id), 'multiple build steps as latest retry'
    FROM build_step
    GROUP BY build_id, build_step_key, shard_index
    HAVING sum(CASE WHEN is_latest_retry THEN 1 ELSE 0 END) > 1
;
