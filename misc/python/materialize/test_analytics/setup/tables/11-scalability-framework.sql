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


-- result of individual workloads
CREATE TABLE scalability_framework_result (
   build_job_id TEXT NOT NULL,
   framework_version TEXT NOT NULL,
   workload_name TEXT NOT NULL,
   workload_group TEXT NOT NULL,
   workload_version TEXT NOT NULL,
   concurrency INT NOT NULL,
   count INT,
   tps DOUBLE
);

CREATE INDEX pk_scalability_framework_result IN CLUSTER test_analytics ON scalability_framework_result(build_job_id, framework_version, workload_name, workload_version, concurrency);

CREATE INDEX fk_scalability_framework_result_build_job_id IN CLUSTER test_analytics ON scalability_framework_result(build_job_id);
