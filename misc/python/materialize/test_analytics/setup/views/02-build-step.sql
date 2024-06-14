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

-- meta data of (the latest retry of) each eventually successful build step
CREATE OR REPLACE VIEW v_successful_build_steps AS
SELECT
    b.build_id,
    bs.build_step_id,
    b.branch,
    b.pipeline,
    b.build_number,
    b.commit_hash,
    b.mz_version,
    b.date,
    b.build_url,
    bs.build_step_key,
    bs.shard_index
FROM build b
INNER JOIN build_step bs
  ON b.build_id = bs.build_id
WHERE bs.success = TRUE
  AND bs.is_latest_retry = TRUE;

CREATE OR REPLACE VIEW v_build_step_success AS
SELECT
    b.branch,
    b.pipeline,
    bs.build_step_key,
    bs.shard_index,
    count(*) AS count_all,
    sum(CASE WHEN bs.success THEN 1 ELSE 0 END) AS count_successful,
    avg(bs.retry_count) AS mean_retry_count
FROM build b
INNER JOIN build_step bs
  ON b.build_id = bs.build_id
WHERE bs.is_latest_retry = TRUE
GROUP BY
    b.branch,
    b.pipeline,
    bs.build_step_key,
    bs.shard_index
;
