-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.


CREATE TABLE console_scalability_result (
   build_job_id TEXT NOT NULL,
   framework_version TEXT NOT NULL,
   workload TEXT NOT NULL,
   phase TEXT NOT NULL,
   viewers INT,
   seconds DOUBLE NOT NULL,
   catalog_server_size TEXT NOT NULL,
   catalog_server_cores DOUBLE,
   environmentd_cores DOUBLE,
   scheduled_cores DOUBLE NOT NULL,
   open_subscribes INT NOT NULL,
   succeeded BOOLEAN NOT NULL
);

ALTER TABLE console_scalability_result OWNER TO qa;
GRANT SELECT, INSERT, UPDATE ON TABLE console_scalability_result TO "hetzner-ci";
