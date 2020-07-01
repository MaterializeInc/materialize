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

# Test AS OF and WITH/WITHOUT SNAPSHOT.
> CREATE MATERIALIZED SOURCE dynamic_src
  FROM FILE '${testdrive.temp-dir}/test.csv' WITH (tail = true)
  FORMAT CSV WITH HEADER

# Ensure the data is read into the source before creating sinks from it
# to correctly test WITH/WITHOUT SNAPSHOT.
> SELECT * FROM dynamic_src
jack jill 2
goofus gallant 3

> CREATE SINK snk7 FROM dynamic_src
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'snk7'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  WITHOUT SNAPSHOT

> CREATE SINK snk8 FROM dynamic_src
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'snk8'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  WITH SNAPSHOT

$ file-append path=test.csv
extra,row

$ kafka-verify format=avro sink=materialize.public.snk7
{"before": null, "after": {"a": "extra", "b": "row", "mz_line_no": 4}}

$ kafka-verify format=avro sink=materialize.public.snk8
{"before": null, "after": {"a": "jack", "b": "jill", "mz_line_no": 2}}
{"before": null, "after": {"a": "goofus", "b": "gallant", "mz_line_no": 3}}
{"before": null, "after": {"a": "extra", "b": "row", "mz_line_no": 4}}

# Test that we are correctly handling WITH/WITHOUT SNAPSHOT.
> CREATE MATERIALIZED VIEW foo AS VALUES (1), (2), (3);

> CREATE SINK sink9 FROM foo
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'sink9'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  WITHOUT SNAPSHOT

$ kafka-verify format=avro sink=materialize.public.sink9

> CREATE SINK sink10 FROM foo
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'sink10'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  WITH SNAPSHOT

$ kafka-verify format=avro sink=materialize.public.sink10
{"before": null, "after": {"column1": 1}}
{"before": null, "after": {"column1": 2}}
{"before": null, "after": {"column1": 3}}
