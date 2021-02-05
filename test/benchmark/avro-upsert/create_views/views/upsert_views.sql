-- Copyright Materialize, Inc. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file at the root of this repository.
--
-- As of the Change Date specified in that file, in accordance with
-- the Business Source License, use of this software will be governed
-- by the Apache License, Version 2.0.

CREATE SOURCE source_upsertavrotest
FROM KAFKA BROKER 'kafka:9092'
TOPIC 'upsertavrotest'
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY 'http://schema-registry:8081'
ENVELOPE UPSERT
FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY 'http://schema-registry:8081';

CREATE VIEW upsertavrotest AS SELECT
    "Key1" AS Key1, -- retaining AS long
    "Key2" AS Key2, -- retaining AS long
    (("OuterRecord")."Record1"."InnerRecord1"."Point")::float AS "Point1", -- converting long to float
    (("OuterRecord")."Record1"."InnerRecord2"."Point")::float AS "Point2", -- converting long to float
    (("OuterRecord")."Record2"."InnerRecord3"."Point")::float AS "Point3", -- converting long to float
    (("OuterRecord")."Record2"."InnerRecord4"."Point")::float AS "Point4" -- converting long to float
FROM source_upsertavrotest;

CREATE INDEX upsertavrotest_index ON upsertavrotest(Key1, Key2);
