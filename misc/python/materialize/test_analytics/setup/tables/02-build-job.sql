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

-- meta data of the build step
CREATE TABLE build_job (
    -- build_job_id is assumed to be globally unique (build_step_id is reused on shared and retried build jobs)
    build_job_id TEXT NOT NULL,
    build_step_id TEXT NOT NULL,
    build_id TEXT NOT NULL,
    build_step_key TEXT NOT NULL,
    shard_index UINT4,
    retry_count UINT4 NOT NULL,
    insert_date TIMESTAMPTZ NOT NULL,
    is_latest_retry BOOL NOT NULL,
    success BOOL NOT NULL,
    aws_instance_type TEXT NOT NULL,
    remarks TEXT
);

CREATE INDEX pk_build_job IN CLUSTER test_analytics ON build_job(build_job_id);

CREATE INDEX fk_build_job_build IN CLUSTER test_analytics ON build_job(build_id);
