-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

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

CREATE INDEX v_scalability_framework_result_per_day ON v_scalability_framework_result_per_day
(
    branch,
    workload_name,
    workload_group,
    framework_version,
    workload_version,
    concurrency,
    day,
    min_count,
    max_count,
    min_tps,
    max_tps
);

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

CREATE OR REPLACE VIEW v_scalability_framework_result_per_month AS
    SELECT
        b.branch,
        res.workload_name,
        res.workload_group,
        res.framework_version,
        res.workload_version,
        res.concurrency,
        date_trunc('month', b.date) AS month,
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
        date_trunc('month', b.date)
;
