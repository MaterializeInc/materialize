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
      bj.build_id,
      bmc.build_job_id,
      bmc.framework_version,
      bmc.scenario_name,
      bmc.minimization_target,
      min(bmc.tested_memory_mz_in_gb) AS min_tested_memory_mz_in_gb,
      min(bmc.tested_memory_clusterd_in_gb) AS min_tested_memory_clusterd_in_gb,
     status
    FROM bounded_memory_config bmc
    INNER JOIN build_job bj
    ON bmc.build_job_id = bj.build_job_id
    GROUP BY
      bj.build_id,
      bmc.build_job_id,
      bmc.framework_version,
      bmc.scenario_name,
      bmc.minimization_target,
      bmc.status
    ;

ALTER VIEW v_bounded_memory_minimal_config OWNER TO qa;
