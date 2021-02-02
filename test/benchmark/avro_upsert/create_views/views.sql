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
    -- cast("BookId" AS bigint) AS "BookId",
    -- cast("SecurityId" AS bigint) AS "SecurityId",
    -- cast("TradingBookId" AS int) AS "TradingBookId",
    "BookId" AS BookId, -- retaining AS long
    "SecurityId" AS SecurityId, -- retaining AS long
    (("Exposure")."Current"."Long2"."Exposure")::float AS "CurrentLongExposure", -- converting long to float
    (("Exposure")."Current"."Short2"."Exposure")::float AS "CurrentShortExposure", -- converting long to float
    (("Exposure")."Target"."Long"."Exposure")::float AS "TargetLongExposure", -- converting long to float
    (("Exposure")."Target"."Short"."Exposure")::float AS "TargetShortExposure" -- converting long to float
FROM source_upsertavrotest;

CREATE INDEX upsertavrotest_index ON upsertavrotest(BookId, SecurityId);
