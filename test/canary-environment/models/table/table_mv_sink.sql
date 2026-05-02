-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

{{ config(materialized='sink', cluster='qa_canary_environment_storage') }}
FROM {{ ref('table_mv') }}
INTO KAFKA CONNECTION kafka_connection (TOPIC 'table_mv')
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection
ENVELOPE DEBEZIUM;
