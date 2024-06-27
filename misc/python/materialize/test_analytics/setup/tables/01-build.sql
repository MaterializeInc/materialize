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
   branch TEXT NOT NULL,
   commit_hash TEXT NOT NULL,
   main_ancestor_commit_hash TEXT, -- nullable for now not to break earlier versions
   mz_version TEXT NOT NULL,
   date TIMESTAMPTZ NOT NULL,
   build_url TEXT, -- nullable, will eventually be removed
   data_version UINT4 NOT NULL,
   remarks TEXT
);

CREATE INDEX pk_build IN CLUSTER test_analytics ON build(build_id);
