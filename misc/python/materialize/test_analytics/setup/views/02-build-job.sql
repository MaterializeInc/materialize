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

CREATE OR REPLACE VIEW v_build_job_success_unsharded AS
SELECT
    bj.build_id,
    bj.build_step_key,
    date_trunc('day', min(bj.start_time)) AS day,
    -- success when no shard failed
    sum(CASE WHEN bj.success THEN 0 ELSE 1 END) = 0 AS success,
    sum(extract(EPOCH FROM (bj.end_time - bj.start_time))) AS duration_in_sec,
    count(*) as count_shards
FROM build_job bj
WHERE bj.is_latest_retry = TRUE
GROUP BY
    bj.build_id,
    bj.build_step_key
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

ALTER VIEW v_successful_build_jobs OWNER TO qa;
ALTER VIEW v_build_job_success_unsharded OWNER TO qa;
ALTER MATERIALIZED VIEW mv_recent_build_job_success_on_main_v2 OWNER TO qa;
GRANT SELECT ON TABLE mv_recent_build_job_success_on_main_v2 TO "hetzner-ci";
