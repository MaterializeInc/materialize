-- Copyright Materialize, Inc. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

CREATE SOURCE time_per_operator_v0
FROM KAFKA BROKER 'kafka:9093'
TOPIC 'dev.mtrlz.time_per_operator.v0'
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY 'http://schema-registry:8081'
ENVELOPE NONE;

CREATE SOURCE time_per_operator_per_worker_v0
FROM KAFKA BROKER 'kafka:9093'
TOPIC 'dev.mtrlz.time_per_operator_per_worker.v0'
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY 'http://schema-registry:8081'
ENVELOPE NONE;

CREATE SOURCE time_per_worker_v0
FROM KAFKA BROKER 'kafka:9093'
TOPIC 'dev.mtrlz.time_per_worker.v0'
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY 'http://schema-registry:8081'
ENVELOPE NONE;

CREATE MATERIALIZED VIEW IF NOT EXISTS time_per_operator_per_worker AS
    SELECT * FROM time_per_operator_per_worker_v0;

CREATE MATERIALIZED VIEW IF NOT EXISTS time_per_operator AS
    SELECT * FROM time_per_operator_v0;

CREATE MATERIALIZED VIEW IF NOT EXISTS time_per_worker AS
    SELECT mz_cluster_id, mz_scheduling_worker_id, mz_scheduling_elapsed_ns FROM time_per_worker_v0
    WHERE mz_logical_timestamp = (SELECT max(mz_logical_timestamp) FROM time_per_worker_v0);
