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
    max(b.build_id)
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
