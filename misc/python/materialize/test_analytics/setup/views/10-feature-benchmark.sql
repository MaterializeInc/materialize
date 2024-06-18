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
        branch,
        scenario_name,
        framework_version,
        scenario_version,
        scale,
        date_trunc('day', date) AS day,
        max(wallclock) AS max_wallclock,
        max(messages) AS max_messages,
        max(memory_mz) AS max_memory_mz,
        max(memory_clusterd) AS max_memory_clusterd
    FROM feature_benchmark_result r
    INNER JOIN build b
    ON b.build_id = r.build_id
    INNER JOIN build_job bs
    ON bs.build_job_id = r.build_job_id
    WHERE bs.is_latest_retry = TRUE
    GROUP BY
        branch,
        scenario_name,
        framework_version,
        scenario_version,
        scale,
        date_trunc('day', date)
;

CREATE OR REPLACE VIEW v_feature_benchmark_result_per_week AS
    SELECT
        branch,
        scenario_name,
        framework_version,
        scenario_version,
        scale,
        date_trunc('week', date) AS day,
        max(wallclock) AS max_wallclock,
        max(messages) AS max_messages,
        max(memory_mz) AS max_memory_mz,
        max(memory_clusterd) AS max_memory_clusterd
    FROM feature_benchmark_result r
    INNER JOIN build b
    ON b.build_id = r.build_id
    INNER JOIN build_job bs
    ON bs.build_job_id = r.build_job_id
    WHERE bs.is_latest_retry = TRUE
    GROUP BY
        branch,
        scenario_name,
        framework_version,
        scenario_version,
        scale,
        date_trunc('week', date)
;
