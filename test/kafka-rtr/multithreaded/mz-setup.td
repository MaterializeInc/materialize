# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ kafka-create-topic topic=input_1

$ kafka-ingest topic=input_1 format=bytes repeat=100
A,B,0

$ kafka-create-topic topic=input_2

$ kafka-ingest topic=input_2 format=bytes repeat=100
A,B,0

> CREATE CONNECTION IF NOT EXISTS kafka_conn_1 TO KAFKA (BROKER 'kafka:9092', SECURITY PROTOCOL PLAINTEXT);
> CREATE CONNECTION IF NOT EXISTS kafka_conn_2 TO KAFKA (BROKER 'kafka:9092', SECURITY PROTOCOL PLAINTEXT);

> CREATE SOURCE input_1 (city, state, zip)
  FROM KAFKA CONNECTION kafka_conn_1 (TOPIC 'testdrive-input_1-${testdrive.seed}')
  FORMAT CSV WITH 3 COLUMNS

> CREATE SOURCE input_2 (city, state, zip)
  FROM KAFKA CONNECTION kafka_conn_2 (TOPIC 'testdrive-input_2-${testdrive.seed}')
  FORMAT CSV WITH 3 COLUMNS

> CREATE TABLE t (a int);
> INSERT INTO t VALUES (1);

> CREATE MATERIALIZED VIEW sum AS
  SELECT sum(count)
  FROM (
      SELECT count(*) FROM input_1
      UNION ALL SELECT count(*) FROM input_2
      UNION ALL SELECT count(*) FROM t
  ) AS x;
