-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

-- meta data of (the latest retry of) each eventually successful build job
CREATE OR REPLACE VIEW v_successful_build_jobs AS
SELECT
    b.build_id,
    bj.build_job_id,
    bj.build_step_id,
    b.branch,
    b.pipeline,
    b.build_number,
    b.commit_hash,
    b.mz_version,
    b.date,
    concat('https://buildkite.com/materialize/', b.pipeline, '/builds/', b.build_number, '#', bj.build_job_id),
    bj.build_step_key,
    bj.shard_index
FROM build b
INNER JOIN build_job bj
  ON b.build_id = bj.build_id
WHERE bj.success = TRUE
  AND bj.is_latest_retry = TRUE;

CREATE OR REPLACE VIEW v_build_step_success AS
SELECT
    EXTRACT(YEAR FROM b.date) AS year,
    EXTRACT(MONTH FROM b.date) AS month,
    b.branch,
    b.pipeline,
    bj.build_step_key,
    bj.shard_index,
    count(*) AS count_all,
    sum(CASE WHEN bj.success THEN 1 ELSE 0 END) AS count_successful,
    -- avg(bj.end_time - bj.start_time) AS mean_duration_on_success, -- TODO #28559: avg(interval) not supported
    avg(bj.retry_count) AS mean_retry_count
FROM build b
INNER JOIN build_job bj
  ON b.build_id = bj.build_id
WHERE bj.is_latest_retry = TRUE
GROUP BY
    EXTRACT(YEAR FROM b.date),
    EXTRACT(MONTH FROM b.date),
    b.branch,
    b.pipeline,
    bj.build_step_key,
    bj.shard_index
;

CREATE OR REPLACE VIEW v_build_step_success_unsharded AS
WITH build_job_unsharded AS (
    SELECT
        bj.build_id,
        bj.build_step_key,
        -- success when no shard failed
        sum(CASE WHEN bj.success THEN 0 ELSE 1 END) = 0 AS success,
        count(*) as count_shards
    FROM build_job bj
    WHERE bj.is_latest_retry = TRUE
    GROUP BY
        bj.build_id,
        bj.build_step_key
)
SELECT
    EXTRACT(YEAR FROM b.date) AS year,
    EXTRACT(MONTH FROM b.date) AS month,
    b.branch,
    b.pipeline,
    bju.build_step_key,
    count(*) AS count_all,
    sum(CASE WHEN bju.success THEN 1 ELSE 0 END) AS count_successful,
    max(bju.count_shards) AS count_shards
FROM build_job_unsharded bju
INNER JOIN build b
  ON b.build_id = bju.build_id
GROUP BY
    EXTRACT(YEAR FROM b.date),
    EXTRACT(MONTH FROM b.date),
    b.branch,
    b.pipeline,
    bju.build_step_key
;

CREATE OR REPLACE MATERIALIZED VIEW mv_recent_build_job_success_on_main_v2
IN CLUSTER test_analytics AS
SELECT * FROM (
    SELECT
        row_number() OVER (
            PARTITION BY pipeline, build_step_key, shard_index
            ORDER BY build_number DESC
        ),
        pipeline,
        build_step_key,
        shard_index,
        build_number,
        build.build_id,
        build_job_id,
        success AS build_step_success
    FROM build_job
    INNER JOIN build
    ON build.build_id = build_job.build_id AND build.branch = 'main' AND is_latest_retry = TRUE
) WHERE row_number <= 5
;

GRANT SELECT ON TABLE mv_recent_build_job_success_on_main_v2 TO "hetzner-ci";
