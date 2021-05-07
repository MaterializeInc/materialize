-- Copyright Materialize, Inc. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

CREATE SOURCE source_aggregationtest
FROM KAFKA BROKER 'kafka:9092'
TOPIC 'aggregationtest'
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY 'http://schema-registry:8081'
ENVELOPE UPSERT
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY 'http://schema-registry:8081';

CREATE VIEW aggregationtest AS SELECT
    sum(
        (("OuterRecord")."DecValue")
    ) AS dec_sum
FROM source_aggregationtest;

CREATE INDEX aggregationtest_index ON aggregationtest(dec_sum);
