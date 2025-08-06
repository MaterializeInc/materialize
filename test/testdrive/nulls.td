# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-arg-default single-replica-cluster=quickstart

$ set foo-schema={
    "name": "row",
    "type": "record",
    "fields": [
      {"name": "a", "type": ["null", "long"]}
    ]
  }

$ kafka-create-topic topic=foo

$ kafka-ingest format=avro topic=foo schema=${foo-schema} timestamp=1
{"a": {"long": 1}}
{"a": {"long": 2}}
{"a": null}

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> BEGIN
> CREATE SOURCE foo
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-foo-${testdrive.seed}')

> CREATE TABLE foo_tbl FROM SOURCE foo (REFERENCE "testdrive-foo-${testdrive.seed}")
  FORMAT AVRO USING SCHEMA '${foo-schema}'
> COMMIT

> SELECT * FROM foo_tbl;
1
2
<null>

> CREATE MATERIALIZED VIEW test1 AS
  SELECT * FROM foo_tbl JOIN foo_tbl as foo2 USING (a);

> SELECT * FROM test1;
a
---
1
2

#In the event of null payload, materialize should skip the entry and not panic
$ kafka-create-topic topic=nullpayload

$ kafka-ingest format=bytes key-format=bytes key-terminator=: topic=nullpayload timestamp=1
:Sé
:
:así

> BEGIN
> CREATE SOURCE nullpayload
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-nullpayload-${testdrive.seed}')

> CREATE TABLE nullpayload_tbl (col) FROM SOURCE nullpayload (REFERENCE "testdrive-nullpayload-${testdrive.seed}")
  FORMAT BYTES
> COMMIT

> SELECT col from nullpayload_tbl
col
---
"S\\xc3\\xa9"
"as\\xc3\\xad"
