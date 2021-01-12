-- Copyright Materialize, Inc. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

CREATE SOURCE IF NOT EXISTS time_per_operator_per_worker_source
FROM AVRO OCF '/metrics/time_per_operator_per_worker.avro' WITH (tail = true);

CREATE MATERIALIZED VIEW IF NOT EXISTS time_per_operator_per_worker AS
    SELECT * FROM time_per_operator_per_worker_source;

CREATE SOURCE IF NOT EXISTS time_per_worker_source
FROM AVRO OCF '/metrics/time_per_worker.avro' WITH (tail = true);

CREATE MATERIALIZED VIEW IF NOT EXISTS time_per_worker AS
    SELECT * FROM time_per_worker_source;

CREATE SOURCE IF NOT EXISTS time_per_operator_source
FROM AVRO OCF '/metrics/time_per_operator.avro' WITH (tail = true);

CREATE MATERIALIZED VIEW IF NOT EXISTS time_per_operator AS
    SELECT * FROM time_per_operator_source;
