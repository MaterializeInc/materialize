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

CREATE TABLE output_consistency_stats (
   build_job_id TEXT NOT NULL,
   framework TEXT NOT NULL,
   count_generated_expressions UINT4 NOT NULL,
   count_ignored_expressions UINT4 NOT NULL,
   count_executed_queries UINT4 NOT NULL,
   count_successful_queries UINT4 NOT NULL,
   count_ignored_error_queries UINT4 NOT NULL,
   count_failed_queries UINT4 NOT NULL,
   count_predefined_queries UINT4 NOT NULL,
   count_available_data_types UINT4 NOT NULL,
   count_available_ops UINT4 NOT NULL,
   count_used_ops UINT4 NOT NULL
);
