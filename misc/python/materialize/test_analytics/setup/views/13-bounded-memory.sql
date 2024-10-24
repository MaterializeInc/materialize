-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

CREATE OR REPLACE VIEW v_bounded_memory_minimal_config AS
    SELECT
      build_job_id,
      framework_version,
      scenario_name,
      minimization_target,
      min(tested_memory_mz_in_gb) AS min_tested_memory_mz_in_gb,
      min(tested_memory_clusterd_in_gb) AS min_tested_memory_clusterd_in_gb,
     status
    FROM bounded_memory_config
    GROUP BY
      build_job_id,
      framework_version,
      scenario_name,
      minimization_target,
      status
    ;

ALTER VIEW v_bounded_memory_minimal_config OWNER TO qa;
