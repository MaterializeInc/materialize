-- Copyright Materialize, Inc. and contributors. All rights reserved.
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
ENVELOPE NONE;

CREATE MATERIALIZED VIEW aggregationtest AS SELECT
        sum(("OuterRecord")."DecValue00") AS dec00_sum,
        sum(("OuterRecord")."DecValue01") AS dec01_sum,
        sum(("OuterRecord")."DecValue02") AS dec02_sum,
        sum(("OuterRecord")."DecValue03") AS dec03_sum,
        sum(("OuterRecord")."DecValue04") AS dec04_sum,
        sum(("OuterRecord")."DecValue05") AS dec05_sum,
        sum(("OuterRecord")."DecValue06") AS dec06_sum,
        sum(("OuterRecord")."DecValue07") AS dec07_sum,
        sum(("OuterRecord")."DecValue08") AS dec08_sum,
        sum(("OuterRecord")."DecValue09") AS dec09_sum,
        sum(("OuterRecord")."DecValue10") AS dec10_sum,
        sum(("OuterRecord")."DecValue11") AS dec11_sum,
        sum(("OuterRecord")."DecValue12") AS dec12_sum,
        sum(("OuterRecord")."DecValue13") AS dec13_sum,
        sum(("OuterRecord")."DecValue14") AS dec14_sum,
        sum(("OuterRecord")."DecValue15") AS dec15_sum,
        sum(("OuterRecord")."DecValue16") AS dec16_sum,
        sum(("OuterRecord")."DecValue17") AS dec17_sum,
        sum(("OuterRecord")."DecValue18") AS dec18_sum,
        sum(("OuterRecord")."DecValue19") AS dec19_sum,
        sum(("OuterRecord")."DecValue20") AS dec20_sum,
        sum(("OuterRecord")."DecValue21") AS dec21_sum,
        sum(("OuterRecord")."DecValue22") AS dec22_sum,
        sum(("OuterRecord")."DecValue23") AS dec23_sum,
        sum(("OuterRecord")."DecValue24") AS dec24_sum,
        sum(("OuterRecord")."DecValue25") AS dec25_sum,
        sum(("OuterRecord")."DecValue26") AS dec26_sum,
        sum(("OuterRecord")."DecValue27") AS dec27_sum,
        sum(("OuterRecord")."DecValue28") AS dec28_sum,
        sum(("OuterRecord")."DecValue29") AS dec29_sum,
        sum(("OuterRecord")."DecValue30") AS dec30_sum,
        sum(("OuterRecord")."DecValue31") AS dec31_sum,
        sum(("OuterRecord")."DecValue32") AS dec32_sum,
        sum(("OuterRecord")."DecValue33") AS dec33_sum,
        sum(("OuterRecord")."DecValue34") AS dec34_sum,
        sum(("OuterRecord")."DecValue35") AS dec35_sum,
        sum(("OuterRecord")."DecValue36") AS dec36_sum,
        sum(("OuterRecord")."DecValue37") AS dec37_sum,
        sum(("OuterRecord")."DecValue38") AS dec38_sum,
        sum(("OuterRecord")."DecValue39") AS dec39_sum,
        sum(("OuterRecord")."DecValue40") AS dec40_sum,
        sum(("OuterRecord")."DecValue41") AS dec41_sum,
        sum(("OuterRecord")."DecValue42") AS dec42_sum,
        sum(("OuterRecord")."DecValue43") AS dec43_sum,
        sum(("OuterRecord")."DecValue44") AS dec44_sum,
        sum(("OuterRecord")."DecValue45") AS dec45_sum,
        sum(("OuterRecord")."DecValue46") AS dec46_sum,
        sum(("OuterRecord")."DecValue47") AS dec47_sum,
        sum(("OuterRecord")."DecValue48") AS dec48_sum,
        sum(("OuterRecord")."DecValue49") AS dec49_sum,
        sum(("OuterRecord")."DecValue50") AS dec50_sum,
        sum(("OuterRecord")."DecValue51") AS dec51_sum,
        sum(("OuterRecord")."DecValue52") AS dec52_sum,
        sum(("OuterRecord")."DecValue53") AS dec53_sum,
        sum(("OuterRecord")."DecValue54") AS dec54_sum,
        sum(("OuterRecord")."DecValue55") AS dec55_sum,
        sum(("OuterRecord")."DecValue56") AS dec56_sum,
        sum(("OuterRecord")."DecValue57") AS dec57_sum,
        sum(("OuterRecord")."DecValue58") AS dec58_sum,
        sum(("OuterRecord")."DecValue59") AS dec59_sum,
        sum(("OuterRecord")."DecValue60") AS dec60_sum,
        sum(("OuterRecord")."DecValue61") AS dec61_sum,
        sum(("OuterRecord")."DecValue62") AS dec62_sum,
        sum(("OuterRecord")."DecValue63") AS dec63_sum,
        sum(("OuterRecord")."DecValue64") AS dec64_sum,
        sum(("OuterRecord")."DecValue65") AS dec65_sum,
        sum(("OuterRecord")."DecValue66") AS dec66_sum,
        sum(("OuterRecord")."DecValue67") AS dec67_sum,
        sum(("OuterRecord")."DecValue68") AS dec68_sum,
        sum(("OuterRecord")."DecValue69") AS dec69_sum,
        sum(("OuterRecord")."DecValue70") AS dec70_sum,
        sum(("OuterRecord")."DecValue71") AS dec71_sum,
        sum(("OuterRecord")."DecValue72") AS dec72_sum,
        sum(("OuterRecord")."DecValue73") AS dec73_sum,
        sum(("OuterRecord")."DecValue74") AS dec74_sum,
        sum(("OuterRecord")."DecValue75") AS dec75_sum,
        sum(("OuterRecord")."DecValue76") AS dec76_sum,
        sum(("OuterRecord")."DecValue77") AS dec77_sum,
        sum(("OuterRecord")."DecValue78") AS dec78_sum,
        sum(("OuterRecord")."DecValue79") AS dec79_sum,
        sum(("OuterRecord")."DecValue80") AS dec80_sum,
        sum(("OuterRecord")."DecValue81") AS dec81_sum,
        sum(("OuterRecord")."DecValue82") AS dec82_sum,
        sum(("OuterRecord")."DecValue83") AS dec83_sum,
        sum(("OuterRecord")."DecValue84") AS dec84_sum,
        sum(("OuterRecord")."DecValue85") AS dec85_sum,
        sum(("OuterRecord")."DecValue86") AS dec86_sum,
        sum(("OuterRecord")."DecValue87") AS dec87_sum,
        sum(("OuterRecord")."DecValue88") AS dec88_sum,
        sum(("OuterRecord")."DecValue89") AS dec89_sum,
        sum(("OuterRecord")."DecValue90") AS dec90_sum,
        sum(("OuterRecord")."DecValue91") AS dec91_sum,
        sum(("OuterRecord")."DecValue92") AS dec92_sum,
        sum(("OuterRecord")."DecValue93") AS dec93_sum,
        sum(("OuterRecord")."DecValue94") AS dec94_sum,
        sum(("OuterRecord")."DecValue95") AS dec95_sum,
        sum(("OuterRecord")."DecValue96") AS dec96_sum,
        sum(("OuterRecord")."DecValue97") AS dec97_sum,
        sum(("OuterRecord")."DecValue98") AS dec98_sum,
        sum(("OuterRecord")."DecValue99") AS dec99_sum
FROM source_aggregationtest
GROUP BY ("Key1Unused");
