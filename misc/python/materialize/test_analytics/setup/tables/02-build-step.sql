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

-- meta data of the build
CREATE TABLE build (
   pipeline TEXT NOT NULL,
   build_number UINT4 NOT NULL,
   build_id TEXT NOT NULL,
   branch TEXT, -- should eventually be changed to NOT NULL (but will break on versions that do not set it)
   commit_hash TEXT NOT NULL,
   mz_version TEXT, -- should eventually be changed to NOT NULL (but will break on versions that do not set it)
   date TIMESTAMPTZ NOT NULL,
   build_url TEXT NOT NULL,
   data_version TEXT, -- should eventually be changed to INT and NOT NULL (but will break on versions that do not set it)
   remarks TEXT
);

-- meta data of the build step
CREATE TABLE build_step (
    -- build_step_id is assumed to be globally unique for now
    build_step_id TEXT NOT NULL,
    build_id TEXT, -- should eventually be changed to NOT NULL (but will break on versions that do not set it)
    pipeline TEXT, -- should eventually be removed (but will break on older versions)
    build_number UINT4, -- should eventually be removed (but will break on older versions)
    build_step_key TEXT NOT NULL,
    shard_index UINT4,
    retry_count UINT4 NOT NULL,
    insert_date TIMESTAMPTZ, -- should eventually be changed to NOT NULL (but will break on versions that do not set it)
    url TEXT NOT NULL,
    is_latest_retry BOOL NOT NULL,
    success BOOL NOT NULL,
    aws_instance_type TEXT NOT NULL,
    remarks TEXT
);
