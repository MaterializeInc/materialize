# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ file-append path=test.csv
a,b
jack,jill
goofus,gallant

> CREATE SOURCE src
  FROM FILE '${testdrive.temp-dir}/test.csv'
  FORMAT CSV WITH HEADER

> CREATE MATERIALIZED SOURCE src_materialized
  FROM FILE '${testdrive.temp-dir}/test.csv'
  FORMAT CSV WITH HEADER

> CREATE VIEW v1 AS
  SELECT a || b AS c FROM src

> CREATE VIEW v2 AS
  SELECT a || b AS c FROM src_materialized

> CREATE MATERIALIZED VIEW v3 AS
  SELECT a || b AS c FROM src

# N.B. it is important to test sinks that depend on sources directly vs. sinks
# that depend on views, as the code paths are different.

> CREATE SINK snk1 FROM src
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'snk1'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'

> CREATE SINK snk2 FROM src_materialized
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'snk2'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'

> CREATE SINK snk3 FROM v1
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'snk3'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'

> CREATE SINK snk4 FROM v2
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'snk4'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'

> CREATE SINK snk5 FROM v3
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'snk5'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'

$ kafka-verify format=avro sink=materialize.public.snk1
{"before": null, "after": {"a": "jack", "b": "jill", "mz_line_no": 2}}
{"before": null, "after": {"a": "goofus", "b": "gallant", "mz_line_no": 3}}

$ kafka-verify format=avro sink=materialize.public.snk2
{"before": null, "after": {"a": "jack", "b": "jill", "mz_line_no": 2}}
{"before": null, "after": {"a": "goofus", "b": "gallant", "mz_line_no": 3}}

$ kafka-verify format=avro sink=materialize.public.snk3
{"before": null, "after": {"c": "jackjill"}}
{"before": null, "after": {"c": "goofusgallant"}}

$ kafka-verify format=avro sink=materialize.public.snk4
{"before": null, "after": {"c": "jackjill"}}
{"before": null, "after": {"c": "goofusgallant"}}

$ kafka-verify format=avro sink=materialize.public.snk5
{"before": null, "after": {"c": "jackjill"}}
{"before": null, "after": {"c": "goofusgallant"}}

# Test the case where we have non +/- 1 multiplicities

> CREATE MATERIALIZED VIEW v4 AS
  SELECT true AS c FROM src

> CREATE SINK snk6 FROM v4
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'snk6'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'

$ kafka-verify format=avro sink=materialize.public.snk6
{"before": null, "after": {"c": true}}
{"before": null, "after": {"c": true}}

! CREATE SINK should_fail FROM v4
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'snk7'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  AS OF 123
AS OF not yet supported
