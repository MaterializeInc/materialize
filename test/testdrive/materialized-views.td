
# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Additional test for materialized views, on top of those in
# test/sqllogictest/materialized_views.slt



# Kafka source as a source for a materialized view

$ set materialized-views={
        "type" : "record",
        "name" : "test",
        "fields" : [
            {"name":"f1", "type":"string"}
        ]
    }

$ kafka-create-topic topic=materialized-views

$ kafka-ingest format=avro topic=materialized-views schema=${materialized-views}
{"f1": "123"}

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );

> CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );

> CREATE SOURCE s1
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-materialized-views-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE NONE

$ kafka-ingest format=avro topic=materialized-views schema=${materialized-views}
{"f1": "234"}

> SELECT COUNT(*) FROM s1;
2

> CREATE MATERIALIZED VIEW v1 AS SELECT COUNT(f1::integer) AS c1 FROM s1;

$ kafka-ingest format=avro topic=materialized-views schema=${materialized-views}
{"f1": "345"}

> SELECT * FROM v1;
3

> CREATE SINK sink1 FROM v1
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-materialized-views-sink-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE DEBEZIUM

$ kafka-verify-topic sink=materialize.public.sink1 await-value-schema=true

> CREATE SOURCE sink1_check
  FROM KAFKA CONNECTION kafka_conn (
    TOPIC 'testdrive-materialized-views-sink-${testdrive.seed}'
  )
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE NONE

$ kafka-ingest format=avro topic=materialized-views schema=${materialized-views}
{"f1": "456"}



> SELECT * FROM v1;
4

> SELECT MAX((after).c1) FROM sink1_check;
4

# Inject failure in the source

$ kafka-ingest format=avro topic=materialized-views schema=${materialized-views}
{"f1": "ABC"}

! SELECT * FROM v1;
contains: invalid input syntax for type integer

# REFRESH EVERY

$ postgres-execute connection=postgres://mz_system@${testdrive.materialize-internal-sql-addr}/materialize
ALTER SYSTEM SET enable_refresh_every_mvs = true

> CREATE TABLE t1(x int);

> INSERT INTO t1 VALUES (1);

> CREATE MATERIALIZED VIEW mv1 WITH (REFRESH EVERY '6sec') AS SELECT x+x as x2 FROM t1;

> INSERT INTO t1 VALUES (3);

# The following will not immediately return the recently inserted values, but Testdrive will wait.
> SELECT * FROM mv1;
2
6

> INSERT INTO t1 VALUES (5);

# What we just inserted shouldn't be in the mv yet, because we are just after a refresh (because the previous SELECT
# returned correct results only after a refresh).
> SELECT * FROM mv1;
2
6

> SELECT * FROM mv1;
2
6
10

> DELETE FROM t1;

# Check that I can query it together with other objects, even between refreshes, and that data added later than the last
# refresh in other objects is reflected in the result.
> CREATE MATERIALIZED VIEW mv2 WITH (REFRESH EVERY '10000sec') AS SELECT x+x as x2 FROM t1;

> CREATE TABLE t2(y int);

> INSERT INTO t2 VALUES (100);

> (SELECT * from mv2) UNION (SELECT * FROM t2);
2
6
10
100

######### This was the failing test when I tried to do the initial refresh at the initial since.
## Check that there is an implicit refresh immediately at the creation of the MV, even if it's REFRESH EVERY.
#> CREATE MATERIALIZED VIEW mv3 WITH (REFRESH EVERY '10000sec') AS SELECT y+y as y2 FROM t2;
#
#> SELECT * FROM mv3;
#200
