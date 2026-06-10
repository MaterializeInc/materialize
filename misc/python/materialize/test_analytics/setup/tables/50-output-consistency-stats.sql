-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

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

ALTER TABLE output_consistency_stats OWNER TO qa;
GRANT SELECT, INSERT, UPDATE ON TABLE output_consistency_stats TO "hetzner-ci";
