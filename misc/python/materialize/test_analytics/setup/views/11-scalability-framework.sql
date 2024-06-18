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

CREATE OR REPLACE VIEW v_scalability_framework_result_per_day AS
    SELECT
        branch,
        workload_name,
        workload_group,
        framework_version,
        workload_version,
        concurrency,
        date_trunc('day', date) AS day,
        min(count) AS min_count,
        min(tps) AS min_tps
    FROM scalability_framework_result r
    INNER JOIN build b
    ON b.build_id = r.build_id
    INNER JOIN build_job bj
    ON bj.build_job_id = r.build_job_id
    WHERE bj.is_latest_retry = TRUE
    GROUP BY
        branch,
        workload_name,
        workload_group,
        framework_version,
        workload_version,
        concurrency,
        date_trunc('day', date)
;

CREATE OR REPLACE VIEW v_scalability_framework_result_per_week AS
    SELECT
        branch,
        workload_name,
        workload_group,
        framework_version,
        workload_version,
        concurrency,
        date_trunc('week', date) AS day,
        min(count) AS min_count,
        min(tps) AS min_tps
    FROM scalability_framework_result r
    INNER JOIN build b
    ON b.build_id = r.build_id
    INNER JOIN build_job bj
    ON bj.build_job_id = r.build_job_id
    WHERE bj.is_latest_retry = TRUE
    GROUP BY
        branch,
        workload_name,
        workload_group,
        framework_version,
        workload_version,
        concurrency,
        date_trunc('week', date)
;
