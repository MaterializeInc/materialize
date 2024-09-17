-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

CREATE OR REPLACE VIEW v_build_annotation_error AS
    SELECT
      ann.build_id,
      ann.build_job_id,
      b.pipeline,
      b.build_number,
      bj.build_step_key,
      b.branch,
      b.commit_hash,
      b.mz_version,
      b.date AS build_date,
      ann.insert_date AS annotation_insert_date,
      ann.test_suite,
      ann.test_retry_count,
      err.error_type,
      err.content,
      err.issue,
      err.occurrence_count
    FROM build_annotation ann
    LEFT OUTER JOIN build_annotation_error err
    ON ann.build_job_id = err.build_job_id
    INNER JOIN build b
    ON ann.build_id = b.build_id
    INNER JOIN build_job bj
    ON ann.build_job_id = bj.build_job_id
;

CREATE OR REPLACE VIEW v_build_annotation_overview AS
    SELECT
      bae.build_id,
      bae.build_job_id,
      bae.pipeline,
      bae.build_number,
      bae.branch,
      bae.commit_hash,
      bae.mz_version,
      bae.build_date,
      bae.test_suite,
      bae.test_retry_count,
      count(bae.content) AS distinct_error_count
    FROM v_build_annotation_error bae
    GROUP BY
      bae.build_id,
      bae.build_job_id,
      bae.pipeline,
      bae.build_number,
      bae.branch,
      bae.commit_hash,
      bae.mz_version,
      bae.build_date,
      bae.test_suite,
      bae.test_retry_count
;

ALTER VIEW v_build_annotation_error OWNER TO qa;
ALTER VIEW v_build_annotation_overview OWNER TO qa;
