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


-- result of individual benchmark scenarios
CREATE TABLE feature_benchmark_result (
   build_job_id TEXT NOT NULL,
   framework_version TEXT NOT NULL,
   scenario_name TEXT NOT NULL,
   scenario_group TEXT, -- nullable for now to not break earlier versions
   scenario_version TEXT NOT NULL,
   scale TEXT NOT NULL,
   wallclock DOUBLE,
   messages INT,
   memory_mz DOUBLE,
   memory_clusterd DOUBLE
);
