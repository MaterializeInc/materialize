# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# This whole file is copy and pasted from source-tables.td, with the added force_source_table_syntax flag.

$ set-arg-default default-replica-size=scale=1,workers=1
$ set-arg-default single-replica-cluster=quickstart

#
# Validate feature flag
#

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_create_table_from_source = false

> CREATE SOURCE auction_house
  IN CLUSTER ${arg.single-replica-cluster}
  FROM LOAD GENERATOR AUCTION (AS OF 300, UP TO 301) FOR ALL TABLES;

! CREATE TABLE bids2 FROM SOURCE auction_house (REFERENCE "auction"."bids");
contains: not supported

> DROP SOURCE auction_house CASCADE;

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_create_table_from_source = true
ALTER SYSTEM SET enable_load_generator_key_value = true
ALTER SYSTEM SET force_source_table_syntax = true

#
# Multi-output load generator source using source-fed tables
#

> BEGIN
> CREATE SOURCE auction_house
  IN CLUSTER ${arg.single-replica-cluster}
  FROM LOAD GENERATOR AUCTION (AS OF 300, UP TO 301);

> CREATE TABLE bids FROM SOURCE auction_house (REFERENCE "auction"."bids");
> COMMIT

> SELECT count(*) FROM bids
255

> SELECT status FROM mz_internal.mz_source_statuses WHERE name = 'bids';
running

# Create another table from the same bids upstream using a less qualified reference
> CREATE TABLE bids2 FROM SOURCE auction_house (REFERENCE "bids");

> SHOW TABLES ON auction_house;
bids ""
bids2 ""

> DROP TABLE bids;

> SELECT count(*) FROM bids2
255


# Validate that the available source references table was correctly populated
> SELECT u.name, u.namespace, u.columns
  FROM mz_sources s
  JOIN mz_internal.mz_source_references u ON s.id = u.source_id
  WHERE
  s.name = 'auction_house'
  ORDER BY u.name;
accounts auction {id,org_id,balance}
auctions auction {id,seller,item,end_time}
bids auction {id,buyer,auction_id,amount,bid_time}
organizations auction {id,name}
users auction {id,org_id,name}

> DROP SOURCE auction_house CASCADE;

> SELECT count(*) FROM mz_internal.mz_source_references;
0

#
# Single-output load generator source using source-fed tables
#

> BEGIN
> CREATE SOURCE counter
  IN CLUSTER ${arg.single-replica-cluster}
  FROM LOAD GENERATOR COUNTER (AS OF 4, UP TO 5);

> CREATE TABLE counter_1 FROM SOURCE counter (REFERENCE "counter");

> CREATE TABLE counter_2 FROM SOURCE counter (REFERENCE "counter");
> COMMIT

> SELECT count(*) from counter_1;
5

> SELECT count(*) from counter_2;
5

> SELECT status FROM mz_internal.mz_source_statuses WHERE name = 'counter_2';
running

> SHOW TABLES ON counter;
counter_1 ""
counter_2 ""

> DROP SOURCE counter CASCADE;


#
# Postgres source using source-fed tables
#

> CREATE SECRET pgpass AS 'postgres'

> CREATE CONNECTION pg TO POSTGRES (
    HOST postgres,
    DATABASE postgres,
    USER postgres,
    PASSWORD SECRET pgpass
  )

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER USER postgres WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
CREATE SCHEMA public;

DROP PUBLICATION IF EXISTS mz_source;
CREATE PUBLICATION mz_source FOR ALL TABLES;

CREATE TYPE an_enum AS ENUM ('var0', 'var1');
CREATE TABLE pg_table (a an_enum, b INTEGER);
INSERT INTO pg_table VALUES ('var1', 1234), ('var0', 0);
ALTER TABLE pg_table REPLICA IDENTITY FULL;

> CREATE SOURCE pg_source
  IN CLUSTER ${arg.single-replica-cluster}
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source');

> SHOW SUBSOURCES ON pg_source
pg_source_progress progress

! CREATE TABLE pg_table_1 FROM SOURCE pg_source (REFERENCE "pg_table");
contains:referenced tables use unsupported types

> CREATE TABLE pg_table_1 FROM SOURCE pg_source (REFERENCE "pg_table") WITH (TEXT COLUMNS = (a));

> SELECT * FROM pg_table_1;
var0 0
var1 1234

$ postgres-execute connection=postgres://postgres:postgres@postgres
INSERT INTO pg_table VALUES ('var1', 5678), ('var0', 1);

> SELECT * FROM pg_table_1;
var0 0
var1 1234
var0 1
var1 5678

> SELECT status FROM mz_internal.mz_source_statuses WHERE name = 'pg_table_1';
running

> CREATE TABLE pg_table_2 FROM SOURCE pg_source (REFERENCE "pg_table") WITH (TEXT COLUMNS = (a));

> SELECT * FROM pg_table_2;
var0 0
var1 1234
var0 1
var1 5678


$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE pg_table ADD COLUMN c INTEGER;
INSERT INTO pg_table VALUES ('var0', 5555, 6666), ('var1', 4444, 12);

> CREATE TABLE pg_table_3 FROM SOURCE pg_source (REFERENCE "public"."pg_table") WITH (TEXT COLUMNS = (a));

> SELECT * FROM pg_table_2;
var0 0
var1 1234
var0 1
var1 5678
var0 5555
var1 4444

> SELECT * FROM pg_table_3;
var0 0 <null>
var1 1234 <null>
var0 1 <null>
var1 5678 <null>
var0 5555 6666
var1 4444 12

> CREATE TABLE pg_table_4 (a INTEGER);

! CREATE TABLE pg_table_4 FROM SOURCE pg_source (REFERENCE "public"."pg_table") WITH (TEXT COLUMNS = (a));
contains:already exists

! CREATE TABLE pg_table_5 FROM SOURCE pg_source (REFERENCE "public"."pg_table_5000");
contains:not found in source

$ postgres-execute connection=postgres://postgres:postgres@postgres
CREATE TABLE pg_table_added_later (a INTEGER);
INSERT INTO pg_table_added_later VALUES (555), (666);
ALTER TABLE pg_table_added_later REPLICA IDENTITY FULL;

> CREATE TABLE pg_table_6a FROM SOURCE pg_source (REFERENCE "public"."pg_table_added_later");
> CREATE TABLE pg_table_6b FROM SOURCE pg_source (REFERENCE "public"."pg_table_added_later");

> SELECT * FROM pg_table_6a;
555
666

$ postgres-execute connection=postgres://postgres:postgres@postgres
INSERT INTO pg_table_added_later VALUES (777);

> SELECT * FROM pg_table_6a;
555
666
777

> SELECT * FROM pg_table_6b;
555
666
777

> SHOW TABLES on pg_source;
pg_table_1 ""
pg_table_2 ""
pg_table_3 ""
pg_table_6a ""
pg_table_6b ""

> DROP SOURCE pg_source CASCADE;


#
# MySql source using source-fed tables
#

> CREATE SECRET mysqlpass AS 'p@ssw0rd';

> CREATE CONNECTION mysql_conn TO MYSQL (
    HOST mysql,
    USER root,
    PASSWORD SECRET mysqlpass
  )

$ mysql-connect name=mysql url=mysql://root@mysql password=p@ssw0rd

$ mysql-execute name=mysql
DROP DATABASE IF EXISTS public;
CREATE DATABASE public;
USE public;

CREATE TABLE mysql_table (a ENUM ('var0', 'var1'), b INTEGER);
INSERT INTO mysql_table VALUES ('var1', 1234), ('var0', 0);

> CREATE SOURCE mysql_source
  IN CLUSTER ${arg.single-replica-cluster}
  FROM MYSQL CONNECTION mysql_conn;

> SHOW SUBSOURCES ON mysql_source
mysql_source_progress progress

> CREATE TABLE mysql_table_1 FROM SOURCE mysql_source (REFERENCE "public"."mysql_table");

> SELECT * FROM mysql_table_1;
1 0
2 1234

> CREATE TABLE mysql_table_1_string FROM SOURCE mysql_source (REFERENCE "public"."mysql_table") WITH (TEXT COLUMNS = (a));

> SELECT * FROM mysql_table_1_string;
var0 0
var1 1234

> SELECT status FROM mz_internal.mz_source_statuses WHERE name = 'mysql_table_1' OR name = 'mysql_table_1_string';
running
running

$ mysql-execute name=mysql
INSERT INTO mysql_table VALUES ('var1', 5678), ('var0', 1);

> SELECT * FROM mysql_table_1;
1 0
2 1234
1 1
2 5678

> SELECT * FROM mysql_table_1_string;
var0 0
var1 1234
var0 1
var1 5678

> CREATE TABLE mysql_table_2 FROM SOURCE mysql_source (REFERENCE "public"."mysql_table") WITH (TEXT COLUMNS = (a));

> SELECT * FROM mysql_table_2;
var0 0
var1 1234
var0 1
var1 5678

> DROP TABLE mysql_table_1;

$ mysql-execute name=mysql
ALTER TABLE mysql_table ADD COLUMN c INTEGER;
INSERT INTO mysql_table VALUES ('var0', 5555, 6666), ('var1', 4444, 12);

> CREATE TABLE mysql_table_3 FROM SOURCE mysql_source (REFERENCE "public"."mysql_table") WITH (TEXT COLUMNS = (a));

> SELECT * FROM mysql_table_2;
var0 0
var1 1234
var0 1
var1 5678
var0 5555
var1 4444

> SELECT * FROM mysql_table_3;
var0 0 <null>
var1 1234 <null>
var0 1 <null>
var1 5678 <null>
var0 5555 6666
var1 4444 12

# Validate that the available source references table reflects the state of the upstream
# when the create source was run
> SELECT u.name, u.namespace, u.columns
  FROM mz_sources s
  JOIN mz_internal.mz_source_references u ON s.id = u.source_id
  WHERE
  s.name = 'mysql_source'
  ORDER BY u.name;
mysql_table public {a,b}

# Create a new table in MySQL and refresh to see if the new available reference is picked up
$ mysql-execute name=mysql
USE public;
CREATE TABLE mysql_table_new (foo INTEGER, bar INTEGER);

> ALTER SOURCE mysql_source REFRESH REFERENCES;

> SELECT u.name, u.namespace, u.columns
  FROM mz_sources s
  JOIN mz_internal.mz_source_references u ON s.id = u.source_id
  WHERE
  s.name = 'mysql_source'
  ORDER BY u.name;
mysql_table public {a,b,c}
mysql_table_new public {foo,bar}

> SHOW TABLES ON mysql_source;
mysql_table_1_string ""
mysql_table_2 ""
mysql_table_3 ""

> DROP SOURCE mysql_source CASCADE;

# ensure that CASCADE propagates to the tables
! SELECT * FROM mysql_table_3;
contains:unknown catalog item 'mysql_table_3'

> SELECT count(*) FROM mz_internal.mz_source_references;
0

#
# Kafka source using source-fed tables
#

$ set keyschema={
    "type": "record",
    "name": "Key",
    "fields": [
        {"name": "key", "type": "string"}
    ]
  }

$ set schema={
        "type" : "record",
        "name" : "test",
        "fields" : [
            {"name":"f1", "type":"string"},
            {"name":"f2", "type":"long"}
        ]
    }

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> CREATE CONNECTION IF NOT EXISTS csr_conn TO CONFLUENT SCHEMA REGISTRY (
    URL '${testdrive.schema-registry-url}'
  );

$ kafka-create-topic topic=avroavro

$ kafka-ingest format=avro topic=avroavro key-format=avro key-schema=${keyschema} schema=${schema}
{"key": "fish"} {"f1": "fish", "f2": 1000}
{"key": "bird1"} {"f1":"goose", "f2": 1}
{"key": "birdmore"} {"f1":"geese", "f2": 2}
{"key": "mammal1"} {"f1": "moose", "f2": 1}
{"key": "bird1"}
{"key": "birdmore"} {"f1":"geese", "f2": 56}
{"key": "mammalmore"} {"f1": "moose", "f2": 42}
{"key": "mammal1"}
{"key": "mammalmore"} {"f1":"moose", "f2": 2}

> BEGIN
> CREATE SOURCE avro_source
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-avroavro-${testdrive.seed}');

> CREATE TABLE avro_table_upsert FROM SOURCE avro_source (REFERENCE "testdrive-avroavro-${testdrive.seed}")
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE UPSERT

> CREATE TABLE avro_table_append FROM SOURCE avro_source (REFERENCE "testdrive-avroavro-${testdrive.seed}")
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE NONE

> CREATE TABLE avro_table_append_cols (a, b) FROM SOURCE avro_source (REFERENCE "testdrive-avroavro-${testdrive.seed}")
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE NONE
> COMMIT

> SELECT status FROM mz_internal.mz_source_statuses WHERE name = 'avro_table_upsert';
running

> SELECT * from avro_table_upsert
key           f1       f2
---------------------------
fish          fish     1000
birdmore      geese    56
mammalmore    moose    2

> SELECT * from avro_table_append
f1       f2
---------------
fish     1000
geese    2
geese    56
goose    1
moose    1
moose    2
moose    42

> SELECT * from avro_table_append_cols
a       b
---------------
fish     1000
geese    2
geese    56
goose    1
moose    1
moose    2
moose    42

> SHOW TABLES ON avro_source;
avro_table_append ""
avro_table_append_cols ""
avro_table_upsert ""

> DROP SOURCE avro_source CASCADE

#
# Key-value load generator source using source-fed tables
#

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_create_table_from_source = true
ALTER SYSTEM SET enable_load_generator_key_value = true
ALTER SYSTEM SET force_source_table_syntax = true

> BEGIN
> CREATE SOURCE keyvalue
  IN CLUSTER ${arg.single-replica-cluster}
  FROM LOAD GENERATOR KEY VALUE (
    KEYS 16,
    PARTITIONS 4,
    SNAPSHOT ROUNDS 3,
    SEED 123,
    VALUE SIZE 10,
    BATCH SIZE 2,
    TICK INTERVAL '1s'
  );

> CREATE TABLE kv_1 FROM SOURCE keyvalue INCLUDE KEY ENVELOPE UPSERT;

> CREATE TABLE kv_2 FROM SOURCE keyvalue INCLUDE KEY ENVELOPE NONE;
> COMMIT

> SELECT partition, count(*) FROM kv_1 GROUP BY partition
0   4
1   4
2   4
3   4

> SELECT status FROM mz_internal.mz_source_statuses WHERE name = 'kv_1';
running

> SELECT partition, count(*) > 10 FROM kv_2 GROUP BY partition
0   true
1   true
2   true
3   true

> SHOW TABLES ON keyvalue;
kv_1 ""
kv_2 ""

> DROP SOURCE keyvalue CASCADE;

#
# Force usage of the new syntax and check that old statements are disallowed
#

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET force_source_table_syntax = true

! CREATE SOURCE auction_legacy
  IN CLUSTER ${arg.single-replica-cluster}
  FROM LOAD GENERATOR AUCTION (AS OF 300, UP TO 301)
  FOR TABLES (bids AS bids_3);
contains:not supported; use CREATE TABLE .. FROM SOURCE instead

! CREATE SOURCE pg_source_2
  IN CLUSTER ${arg.single-replica-cluster}
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source', TEXT COLUMNS = (public.pg_table.a))
  FOR ALL TABLES
contains:not supported; use CREATE TABLE .. FROM SOURCE instead

! CREATE SOURCE mysql_source_2
  IN CLUSTER ${arg.single-replica-cluster}
  FROM MYSQL CONNECTION mysql_conn
  FOR SCHEMAS (public);
contains:not supported; use CREATE TABLE .. FROM SOURCE instead

! CREATE SOURCE avro_source_2
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-avroavro-${testdrive.seed}')
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_conn
  ENVELOPE UPSERT;
contains:not supported; use CREATE TABLE .. FROM SOURCE instead
