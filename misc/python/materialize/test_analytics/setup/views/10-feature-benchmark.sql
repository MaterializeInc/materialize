-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

CREATE OR REPLACE VIEW v_feature_benchmark_result_per_day AS
    SELECT
        b.branch,
        res.scenario_name,
        res.scenario_group,
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
        res.scenario_group,
        res.framework_version,
        res.scenario_version,
        res.scale,
        date_trunc('day', b.date)
;

CREATE INDEX v_feature_benchmark_result_per_day_idx
IN CLUSTER test_analytics
ON v_feature_benchmark_result_per_day
(
    branch,
    scenario_name,
    scenario_group,
    framework_version,
    scenario_version,
    scale,
    day,
    min_wallclock,
    max_wallclock,
    min_memory_mz,
    max_memory_mz,
    min_memory_clusterd,
    max_memory_clusterd
);

CREATE OR REPLACE VIEW v_feature_benchmark_result_per_week AS
    SELECT
        b.branch,
        res.scenario_name,
        res.scenario_group,
        res.framework_version,
        res.scenario_version,
        res.scale,
        date_trunc('week', b.date) AS week,
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
        res.scenario_group,
        res.framework_version,
        res.scenario_version,
        res.scale,
        date_trunc('week', b.date)
;

CREATE OR REPLACE VIEW v_feature_benchmark_result_per_month AS
    SELECT
        b.branch,
        res.scenario_name,
        res.scenario_group,
        res.framework_version,
        res.scenario_version,
        res.scale,
        date_trunc('month', b.date) AS month,
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
        res.scenario_group,
        res.framework_version,
        res.scenario_version,
        res.scale,
        date_trunc('month', b.date)
;

ALTER VIEW v_feature_benchmark_result_per_day OWNER TO qa;
ALTER VIEW v_feature_benchmark_result_per_week OWNER TO qa;
ALTER VIEW v_feature_benchmark_result_per_month OWNER TO qa;
