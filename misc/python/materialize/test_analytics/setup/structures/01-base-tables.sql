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

-- test history version
CREATE TABLE version(
   version TEXT
);

-- meta data of the build
CREATE TABLE build (
   pipeline TEXT,
   build_number INT,
   build_id TEXT,
   branch TEXT,
   commit_hash TEXT,
   date TIMESTAMPTZ,
   build_url TEXT,
   remarks TEXT
);

-- meta data of the build step
CREATE TABLE build_step (
    -- build_step_id is assumed to be globally unique for now
    build_step_id TEXT,
    pipeline TEXT,
    build_number INT,
    build_step_key TEXT,
    shard_index INT,
    retry_count INT,
    url TEXT,
    is_latest_retry BOOL,
    success BOOL,
    aws_instance_type TEXT,
    remarks TEXT
);
