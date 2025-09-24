-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

-- replace %build-ids% with a criterion

DELETE FROM feature_benchmark_result WHERE build_job_id IN (SELECT build_id FROM build_job WHERE build_id IN (%build-ids%));
DELETE FROM scalability_framework_result WHERE build_job_id IN (SELECT build_id FROM build_job WHERE build_id IN (%build-ids%));
DELETE FROM parallel_benchmark_result WHERE build_job_id IN (SELECT build_id FROM build_job WHERE build_id IN (%build-ids%));
DELETE FROM product_limits_result WHERE build_job_id IN (SELECT build_id FROM build_job WHERE build_id IN (%build-ids%));
DELETE FROM cluster_spec_sheet_result WHERE build_job_id IN (SELECT build_id FROM build_job WHERE build_id IN (%build-ids%));
DELETE FROM build_annotation_error WHERE build_job_id IN (SELECT build_job_id FROM build_annotation WHERE build_id IN (%build-ids%));
DELETE FROM build_annotation WHERE build_id IN (%build-ids%);
DELETE FROM build_job WHERE build_id IN (%build-ids%);
DELETE FROM build WHERE build_id IN (%build-ids%);
