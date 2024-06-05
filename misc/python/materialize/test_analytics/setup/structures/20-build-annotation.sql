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
   type TEXT NOT NULL,
   header TEXT,
   markdown TEXT NOT NULL
);

CREATE VIEW v_build_annotation AS
    SELECT
      ann.build_id,
      b.pipeline,
      b.build_number,
      b.branch,
      b.mz_version,
      b.date,
      ann.type,
      ann.header,
      ann.markdown
    FROM build_annotation ann
    INNER JOIN build b
    ON ann.build_id = b.build_id
;
