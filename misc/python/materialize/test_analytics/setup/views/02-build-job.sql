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
    b.branch,
    b.pipeline,
    bj.build_step_key,
    bj.shard_index
;

-- history of build job success
CREATE OR REPLACE MATERIALIZED VIEW mv_build_job_success IN CLUSTER test_analytics AS
WITH MUTUALLY RECURSIVE data (build_id TEXT, pipeline TEXT, branch TEXT, build_number INT, build_job_id TEXT, build_step_key TEXT, success BOOL, predecessor_index INT, predecessor_build_number INT) AS
(
    SELECT
        bj.build_id, b.pipeline, b.branch, b.build_number, bj.build_job_id, bj.build_step_key, bj.success, 0, b.build_number
    FROM
        build_job bj
    INNER JOIN build b
    ON b.build_id = bj.build_id
    WHERE b.branch = 'main'
    AND bj.is_latest_retry = TRUE
    UNION
    SELECT
        d.build_id, d.pipeline, d.branch, d.build_number, d.build_job_id, d.build_step_key, d.success, d.predecessor_index + 1, max(b2.build_number)
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
        d.predecessor_index,
        d.success
    -- this applies to max(b2.build_number)
    OPTIONS (AGGREGATE INPUT GROUP SIZE = 10)
)
SELECT
    d.build_id,
    d.pipeline,
    d.build_number,
    d.build_job_id,
    d.build_step_key,
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

-- only for backwards compatibility
CREATE OR REPLACE VIEW v_build_job_success AS
SELECT * FROM mv_build_job_success;

CREATE OR REPLACE VIEW v_most_recent_build_job AS
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
INNER JOIN v_most_recent_build mrb
    ON b.pipeline = mrb.pipeline
    AND b.build_number = mrb.highest_build_number
WHERE bj.is_latest_retry = TRUE
;
