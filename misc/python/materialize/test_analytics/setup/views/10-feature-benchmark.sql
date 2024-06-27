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

CREATE OR REPLACE VIEW v_feature_benchmark_result_per_day AS
    SELECT
        b.branch,
        res.scenario_name,
        res.framework_version,
        res.scenario_version,
        res.scale,
        date_trunc('day', b.date) AS day,
        min(res.wallclock) AS min_wallclock,
        avg(res.wallclock) AS avg_wallclock,
        max(res.wallclock) AS max_wallclock,
        min(res.messages) AS min_messages,
        avg(res.messages) AS avg_messages,
        max(res.messages) AS max_messages,
        min(res.memory_mz) AS min_memory_mz,
        avg(res.memory_mz) AS avg_memory_mz,
        max(res.memory_mz) AS max_memory_mz,
        min(res.memory_clusterd) AS min_memory_clusterd,
        avg(res.memory_clusterd) AS avg_memory_clusterd,
        max(res.memory_clusterd) AS max_memory_clusterd
    FROM feature_benchmark_result res
    INNER JOIN build_job bj
    ON bj.build_job_id = res.build_job_id
    INNER JOIN build b
    ON b.build_id = bj.build_id
    WHERE bj.is_latest_retry = TRUE
    GROUP BY
        b.branch,
        res.scenario_name,
        res.framework_version,
        res.scenario_version,
        res.scale,
        date_trunc('day', b.date)
;

CREATE OR REPLACE VIEW v_feature_benchmark_result_per_week AS
    SELECT
        b.branch,
        res.scenario_name,
        res.framework_version,
        res.scenario_version,
        res.scale,
        date_trunc('week', b.date) AS day,
        min(res.wallclock) AS min_wallclock,
        avg(res.wallclock) AS avg_wallclock,
        max(res.wallclock) AS max_wallclock,
        min(res.messages) AS min_messages,
        avg(res.messages) AS avg_messages,
        max(res.messages) AS max_messages,
        min(res.memory_mz) AS min_memory_mz,
        avg(res.memory_mz) AS avg_memory_mz,
        max(res.memory_mz) AS max_memory_mz,
        min(res.memory_clusterd) AS min_memory_clusterd,
        avg(res.memory_clusterd) AS avg_memory_clusterd,
        max(res.memory_clusterd) AS max_memory_clusterd
    FROM feature_benchmark_result res
    INNER JOIN build_job bj
    ON bj.build_job_id = res.build_job_id
    INNER JOIN build b
    ON b.build_id = bj.build_id
    WHERE bj.is_latest_retry = TRUE
    GROUP BY
        b.branch,
        res.scenario_name,
        res.framework_version,
        res.scenario_version,
        res.scale,
        date_trunc('week', b.date)
;
