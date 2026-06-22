-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

CREATE SINK pg_relationships_sink
    IN CLUSTER qa_canary_environment_sinks
    FROM qa_canary_environment.public_pg_cdc_sources.pg_relationships
    INTO KAFKA CONNECTION qa_canary_environment.public.kafka_connection (TOPIC 'pg_relationships')
    FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION qa_canary_environment.public.csr_connection
    ENVELOPE DEBEZIUM;
