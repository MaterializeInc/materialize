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

CREATE TABLE build_annotation (
   build_id TEXT NOT NULL,
   build_step_id TEXT NOT NULL,
   test_suite TEXT NOT NULL,
   test_retry_count UINT4 NOT NULL,
   is_failure BOOL NOT NULL,
   insert_date TIMESTAMPTZ NOT NULL
);

CREATE TABLE build_annotation_error (
   build_step_id TEXT NOT NULL,
   error_type TEXT NOT NULL,
   content TEXT NOT NULL,
   issue TEXT,
   occurrence_count UINT4 NOT NULL
);

CREATE VIEW v_build_annotation_error AS
    SELECT
      ann.build_id,
      ann.build_step_id,
      b.pipeline,
      b.build_number,
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
    ON ann.build_step_id = err.build_step_id
    INNER JOIN build b
    ON ann.build_id = b.build_id
;
