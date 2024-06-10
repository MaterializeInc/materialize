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


-- meta data of (the latest retry of) each eventually successful benchmark build step
CREATE VIEW successful_build_steps AS
SELECT
    b.pipeline,
    b.build_number,
    b.build_id,
    b.commit_hash,
    b.mz_version,
    b.date,
    b.build_url,
    bs.shard_index
FROM build b
INNER JOIN build_step bs
  ON b.build_number = bs.build_number
  AND b.pipeline = bs.pipeline
WHERE bs.success = TRUE
  AND bs.is_latest_retry = TRUE;

CREATE VIEW count_builds_per_week AS
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

CREATE VIEW last_build_per_week AS
SELECT
    EXTRACT(YEAR FROM date) AS year,
    EXTRACT(WEEK FROM date) AS week,
    b.branch,
    b.pipeline,
    max(b.build_id)
FROM build b
GROUP BY
    EXTRACT(YEAR FROM date),
    EXTRACT(WEEK FROM date),
    b.branch,
    b.pipeline;
