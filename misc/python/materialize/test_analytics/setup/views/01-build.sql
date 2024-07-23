-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

CREATE OR REPLACE VIEW v_count_builds_per_week AS
SELECT
    EXTRACT(YEAR FROM date) AS year,
    EXTRACT(WEEK FROM date) AS week,
    b.branch,
    b.pipeline,
    count(b.build_id) AS count,
    max(b.mz_version) AS max_mz_version
FROM build b
GROUP BY
    EXTRACT(YEAR FROM date),
    EXTRACT(WEEK FROM date),
    b.branch,
    b.pipeline;

CREATE OR REPLACE VIEW v_count_builds_per_mz_version AS
SELECT
    b.mz_version,
    b.branch,
    b.pipeline,
    count(b.build_id) AS count,
    min(b.date) AS first_build_date,
    max(b.date) AS last_build_date
FROM build b
WHERE branch = 'main'
GROUP BY
    b.mz_version,
    b.branch,
    b.pipeline;

CREATE OR REPLACE VIEW v_last_build_per_week AS
SELECT
    EXTRACT(YEAR FROM date) AS year,
    EXTRACT(WEEK FROM date) AS week,
    b.branch,
    b.pipeline,
    max(b.build_id) AS build_id
FROM build b
GROUP BY
    EXTRACT(YEAR FROM date),
    EXTRACT(WEEK FROM date),
    b.branch,
    b.pipeline;

CREATE OR REPLACE VIEW v_most_recent_build AS
SELECT
    b.branch,
    b.pipeline,
    max(b.build_number) AS highest_build_number
FROM build b
GROUP BY
    b.branch,
    b.pipeline
;

-- This view can only consider steps that were uploaded to the database.
CREATE OR REPLACE VIEW v_build_success AS
SELECT
    b.pipeline,
    b.branch,
    b.build_id,
    b.build_number,
    b.build_url,
    -- only considers the latest retry
    SUM(CASE WHEN bj.success OR NOT bj.is_latest_retry THEN 0 ELSE 1 END) > 0 AS has_failed_steps,
    -- only considers the latest retry
    SUM(CASE WHEN bj.success OR NOT bj.is_latest_retry THEN 0 ELSE 1 END) AS count_failed_steps,
    -- considers all retry instances
    SUM(CASE WHEN bj.success THEN 0 ELSE 1 END) > 0 AS has_any_failed_jobs
FROM build b
INNER JOIN build_job bj
ON b.build_id = bj.build_id
GROUP BY
    b.pipeline,
    b.branch,
    b.build_id,
    b.build_number,
    b.build_url;
