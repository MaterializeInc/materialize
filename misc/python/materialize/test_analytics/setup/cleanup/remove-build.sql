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

-- replace %build-ids% with a criterion

DELETE FROM feature_benchmark_result WHERE build_id IN (%build-ids%);
DELETE FROM scalability_framework_result WHERE build_id IN (%build-ids%);
DELETE FROM build_annotation_error WHERE build_job_id IN (SELECT build_job_id FROM build_annotation WHERE build_id IN (%build-ids%));
DELETE FROM build_annotation WHERE build_id IN (%build-ids%);
DELETE FROM build_job WHERE build_id IN (%build-ids%);
DELETE FROM build WHERE build_id IN (%build-ids%);
