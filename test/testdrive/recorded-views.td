
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Additional test for recorded views, on top of those in test/sqllogictest/recorded_views.slt



# Kafka source as a source for a recorded view

$ set recorded-views={
        "type" : "record",
        "name" : "test",
        "fields" : [
            {"name":"f1", "type":"string"}
        ]
    }

$ kafka-create-topic topic=recorded-views

$ kafka-ingest format=avro topic=recorded-views schema=${recorded-views} publish=true
{"f1": "123"}

> CREATE MATERIALIZED SOURCE s1
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC
  'testdrive-recorded-views-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE NONE

$ kafka-ingest format=avro topic=recorded-views schema=${recorded-views} publish=true
{"f1": "234"}

> SELECT COUNT(*) FROM s1;
2

> CREATE RECORDED VIEW v1 AS SELECT COUNT(f1::integer) AS c1 FROM s1;

$ kafka-ingest format=avro topic=recorded-views schema=${recorded-views} publish=true
{"f1": "345"}

> CREATE SINK sink1 FROM v1
  INTO KAFKA BROKER '${testdrive.kafka-addr}'
  TOPIC 'recorded-view-sink'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'

$ kafka-ingest format=avro topic=recorded-views schema=${recorded-views} publish=true
{"f1": "456"}

$ set-regex match=\d{13} replacement=<TIMESTAMP>

> SELECT * FROM v1;
4

$ kafka-verify format=avro sink=materialize.public.sink1 sort-messages=true
{"before": null, "after": {"row": {"c1": 2}}}
{"before": {"row": {"c1": 2}}, "after": {"row": {"c1": 4}}}

> BEGIN

> DECLARE c CURSOR FOR TAIL v1;

> FETCH ALL c;
<TIMESTAMP> 1 4

> COMMIT

# Inject failure in the source

$ kafka-ingest format=avro topic=recorded-views schema=${recorded-views} publish=true
{"f1": "ABC"}

# TODO(teskje): Fix propagation of errors through recorded views.
#! SELECT * FROM v1;
#contains: invalid input syntax for type integer
