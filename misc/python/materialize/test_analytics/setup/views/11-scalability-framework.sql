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
        b.branch,
        res.workload_name,
        res.workload_group,
        res.framework_version,
        res.workload_version,
        res.concurrency,
        date_trunc('day', b.date) AS day,
        min(res.count) AS min_count,
        avg(res.count) AS avg_count,
        max(res.count) AS max_count,
        min(res.tps) AS min_tps,
        avg(res.tps) AS avg_tps,
        max(res.tps) AS max_tps
    FROM scalability_framework_result res
    INNER JOIN build_job bj
    ON bj.build_job_id = res.build_job_id
    INNER JOIN build b
    ON b.build_id = bj.build_id
    WHERE bj.is_latest_retry = TRUE
    GROUP BY
        b.branch,
        res.workload_name,
        res.workload_group,
        res.framework_version,
        res.workload_version,
        res.concurrency,
        date_trunc('day', b.date)
;

CREATE OR REPLACE VIEW v_scalability_framework_result_per_week AS
    SELECT
        b.branch,
        res.workload_name,
        res.workload_group,
        res.framework_version,
        res.workload_version,
        res.concurrency,
        date_trunc('week', b.date) AS week,
        min(res.count) AS min_count,
        avg(res.count) AS avg_count,
        max(res.count) AS max_count,
        min(res.tps) AS min_tps,
        avg(res.tps) AS avg_tps,
        max(res.tps) AS max_tps
    FROM scalability_framework_result res
    INNER JOIN build_job bj
    ON bj.build_job_id = res.build_job_id
    INNER JOIN build b
    ON b.build_id = bj.build_id
    WHERE bj.is_latest_retry = TRUE
    GROUP BY
        b.branch,
        res.workload_name,
        res.workload_group,
        res.framework_version,
        res.workload_version,
        res.concurrency,
        date_trunc('week', b.date)
;
