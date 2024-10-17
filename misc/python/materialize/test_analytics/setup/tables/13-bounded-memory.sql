-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.


-- result of minimal memory searches fo bounded-memory
CREATE TABLE bounded_memory_min_found_config (
   build_job_id TEXT NOT NULL,
   framework_version TEXT NOT NULL,
   scenario_name TEXT NOT NULL,
   configured_memory_mz_in_gb DOUBLE NOT NULL,
   configured_memory_clusterd_in_gb DOUBLE NOT NULL,
   found_memory_mz_in_gb DOUBLE NOT NULL,
   found_memory_clusterd_in_gb DOUBLE NOT NULL
);

ALTER TABLE bounded_memory_min_found_config OWNER TO qa;
GRANT SELECT, INSERT, UPDATE ON TABLE bounded_memory_min_found_config TO "hetzner-ci";
