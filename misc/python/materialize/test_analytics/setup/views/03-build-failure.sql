-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

CREATE OR REPLACE MATERIALIZED VIEW mv_build_job_failed_on_branch AS
SELECT build_step_key, branch, part
FROM build
JOIN build_job ON build.build_id = build_job.build_id
JOIN build_job_failure ON build_job.build_job_id = build_job_failure.build_job_id;

CREATE OR REPLACE MATERIALIZED VIEW mv_build_job_failed AS
SELECT build_step_key, part
FROM build_job
JOIN build_job_failure ON build_job.build_job_id = build_job_failure.build_job_id;

CREATE DEFAULT INDEX ON mv_build_job_failed_on_branch;
CREATE DEFAULT INDEX ON mv_build_job_failed;

ALTER MATERIALIZED VIEW mv_build_job_failed_on_branch OWNER TO qa;
GRANT SELECT ON TABLE mv_build_job_failed_on_branch TO "hetzner-ci";
ALTER MATERIALIZED VIEW mv_build_job_failed OWNER TO qa;
GRANT SELECT ON TABLE mv_build_job_failed TO "hetzner-ci";
