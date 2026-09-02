-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

CREATE TABLE customer_tbl FROM SOURCE qa_canary_environment.public_loadgen_sources.customer
    (REFERENCE "qa_canary_customer")
    KEY FORMAT BYTES
    VALUE FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION qa_canary_environment.public.csr_connection
    INCLUDE TIMESTAMP AS kafka_timestamp
    ENVELOPE UPSERT;
