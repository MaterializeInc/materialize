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
