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

-- history of build job success
CREATE OR REPLACE VIEW v_build_job_success AS
WITH MUTUALLY RECURSIVE data (build_id TEXT, pipeline TEXT, branch TEXT, build_number INT, build_job_id TEXT, build_step_key TEXT, date TIMESTAMPTZ, success BOOL, predecessor_index INT, predecessor_build_number INT) AS
(
    SELECT
        bj.build_id, b.pipeline, b.branch, b.build_number, bj.build_job_id, bj.build_step_key, b.date, bj.success, 0, b.build_number
    FROM
        build_job bj
    INNER JOIN build b
    ON b.build_id = bj.build_id
    WHERE bj.is_latest_retry = TRUE
    UNION
    SELECT
        d.build_id, d.pipeline, d.branch, d.build_number, d.build_job_id, d.build_step_key, d.date, d.success, d.predecessor_index + 1, max(b2.build_number)
    FROM
        data d
    INNER JOIN build b2
    ON d.pipeline = b2.pipeline
    AND d.branch = b2.branch
    AND d.predecessor_build_number > b2.build_number
    INNER JOIN build_job bj2
    ON b2.build_id = bj2.build_id
    WHERE d.predecessor_index < 10
    AND bj2.is_latest_retry = TRUE
    GROUP BY
        d.build_id,
        d.pipeline,
        d.branch,
        d.build_number,
        d.build_job_id,
        d.build_step_key,
        d.date,
        d.predecessor_index,
        d.success
    -- this applies to max(b2.build_number)
    OPTIONS (AGGREGATE INPUT GROUP SIZE = 10)
)
SELECT
    d.build_id,
    d.pipeline,
    d.branch,
    d.build_number,
    d.build_job_id,
    d.build_step_key,
    d.date,
    d.success,
    d.predecessor_index,
    d.predecessor_build_number AS predecessor_build_number,
    pred_b.build_id AS predecessor_build_id,
    pred_bj.build_job_id AS predecessor_build_job_id,
    pred_bj.success AS predecessor_build_step_success,
    pred_bj.is_latest_retry AS predecessor_is_latest_retry
FROM
    data d
INNER JOIN build pred_b
ON d.pipeline = pred_b.pipeline
AND d.predecessor_build_number = pred_b.build_number
INNER JOIN build_job pred_bj
ON pred_b.build_id = pred_bj.build_id
AND d.build_step_key = pred_bj.build_step_key
WHERE d.predecessor_index <> 0
;

CREATE OR REPLACE MATERIALIZED VIEW mv_recent_build_job_success_on_main IN CLUSTER test_analytics AS
SELECT
    build_job_id,
    predecessor_build_number,
    predecessor_build_id,
    predecessor_build_job_id,
    predecessor_build_step_success,
    predecessor_index,
    predecessor_is_latest_retry,
    pipeline -- no longer in use, remove eventually
FROM v_build_job_success
WHERE branch = 'main'
AND date + INTERVAL '5' DAY > mz_now()
AND predecessor_index <= 10;

CREATE OR REPLACE VIEW v_most_recent_build_job AS
WITH most_recent_build_with_completed_build_step AS (
    SELECT
        b.branch,
        b.pipeline,
        bj.build_step_key,
        max(b.build_number) AS highest_build_number
    FROM build b
    INNER JOIN build_job bj
    ON b.build_id = bj.build_id
    GROUP BY
        b.branch,
        b.pipeline,
        bj.build_step_key
)
SELECT
    b.pipeline,
    b.branch,
    b.build_id,
    bj.build_job_id,
    bj.build_step_id,
    bj.build_step_key,
    bj.shard_index,
    bj.success
FROM build_job bj
INNER JOIN build b
    ON bj.build_id = b.build_id
INNER JOIN most_recent_build_with_completed_build_step mrb
    ON b.pipeline = mrb.pipeline
    AND b.build_number = mrb.highest_build_number
    AND bj.build_step_key = mrb.build_step_key
WHERE bj.is_latest_retry = TRUE
;
