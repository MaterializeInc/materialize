# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-arg-default default-replica-size=1
$ set-arg-default single-replica-cluster=quickstart

> CREATE SOURCE counter_empty
  IN CLUSTER ${arg.single-replica-cluster}
  FROM LOAD GENERATOR COUNTER (AS OF 5, UP TO 5)

> SELECT count(*) FROM counter_empty
0

> CREATE SOURCE counter_single
  IN CLUSTER ${arg.single-replica-cluster}
  FROM LOAD GENERATOR COUNTER (AS OF 0, UP TO 1)

> SELECT count(*) FROM counter_single
1

> CREATE SOURCE counter_five
  IN CLUSTER ${arg.single-replica-cluster}
  FROM LOAD GENERATOR COUNTER (AS OF 4, UP TO 5)

> SELECT count(*) FROM counter_five
5

! CREATE SOURCE counter
  IN CLUSTER ${arg.single-replica-cluster}
  FROM LOAD GENERATOR COUNTER (AS OF 5, UP TO 4)
contains:UP TO cannot be less than AS OF

! CREATE SOURCE counter
  IN CLUSTER ${arg.single-replica-cluster}
  FROM LOAD GENERATOR COUNTER (SCALE FACTOR 1)
exact:COUNTER load generators do not support SCALE FACTOR values

> DROP SOURCE counter_empty, counter_single, counter_five

> CREATE SOURCE auction_house
  IN CLUSTER ${arg.single-replica-cluster}
  FROM LOAD GENERATOR AUCTION (AS OF 300, UP TO 301) FOR ALL TABLES;

> SHOW CREATE SOURCE auction_house
"materialize.public.auction_house" "CREATE SOURCE \"materialize\".\"public\".\"auction_house\" IN CLUSTER \"${arg.single-replica-cluster}\" FROM LOAD GENERATOR AUCTION (AS OF = 300, UP TO = 301) FOR ALL TABLES EXPOSE PROGRESS AS \"materialize\".\"public\".\"auction_house_progress\""

> SHOW SOURCES
accounts                subsource      ${arg.single-replica-cluster}
auction_house           load-generator ${arg.single-replica-cluster}
auction_house_progress  progress       <null>
auctions                subsource      ${arg.single-replica-cluster}
bids                    subsource      ${arg.single-replica-cluster}
organizations           subsource      ${arg.single-replica-cluster}
users                   subsource      ${arg.single-replica-cluster}

> SELECT count(*) FROM bids
255

# FOR TABLES doesn't always work.
# A previous test here checked bids, which worked.
# It did not check users, which causes a panic.
# If this is fixed in the future, tests should probably exhaustively check each subsource.
! CREATE SOURCE auction_house
  IN CLUSTER ${arg.single-replica-cluster}
  FROM LOAD GENERATOR AUCTION FOR TABLES (user);
contains:LOAD GENERATOR source validation: FOR TABLES (..) unsupported

# For Tables with mentioned schema should work
> CREATE SCHEMA another;
> CREATE SOURCE another.auction_house
  IN CLUSTER ${arg.single-replica-cluster}
  FROM LOAD GENERATOR AUCTION FOR ALL TABLES;

> SHOW SOURCES FROM another;
accounts                subsource      ${arg.single-replica-cluster}
auction_house           load-generator ${arg.single-replica-cluster}
auction_house_progress  progress       <null>
auctions                subsource      ${arg.single-replica-cluster}
bids                    subsource      ${arg.single-replica-cluster}
organizations           subsource      ${arg.single-replica-cluster}
users                   subsource      ${arg.single-replica-cluster}

> CREATE CONNECTION IF NOT EXISTS kafka_conn TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

# Validate that the ID column of the load generator data is usable as a key.
> CREATE SINK accounts_sink
  IN CLUSTER ${arg.single-replica-cluster}
  FROM accounts
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-accounts-${testdrive.seed}')
  KEY (id)
  FORMAT JSON
  ENVELOPE UPSERT;

# Show that AST of subsource contains REFERENCES option
> SHOW CREATE SOURCE accounts
materialize.public.accounts "CREATE SUBSOURCE \"materialize\".\"public\".\"accounts\" (\"id\" \"pg_catalog\".\"int8\" NOT NULL, \"org_id\" \"pg_catalog\".\"int8\" NOT NULL, \"balance\" \"pg_catalog\".\"int8\" NOT NULL, UNIQUE (\"id\")) OF SOURCE \"materialize\".\"public\".\"auction_house\" WITH (EXTERNAL REFERENCE = \"mz_load_generators\".\"auction\".\"accounts\")"

# Check that non-append-only `COUNTER` sources reach the proper size

> CREATE SOURCE counter
  IN CLUSTER ${arg.single-replica-cluster}
  FROM LOAD GENERATOR COUNTER (MAX CARDINALITY 8, TICK INTERVAL '0.001s')

> SELECT count(*) FROM counter
8

# Now make sure it doesn't change
> SELECT mz_unsafe.mz_sleep(1)
<null>

> SELECT count(*) FROM counter
8

# Check that negative max cardinalities are rejected
! CREATE SOURCE counter2
  IN CLUSTER ${arg.single-replica-cluster}
  FROM LOAD GENERATOR COUNTER (MAX CARDINALITY -1)
contains:invalid MAX CARDINALITY: invalid unsigned numeric value: invalid digit found in string

> CREATE SOURCE counter3
  IN CLUSTER ${arg.single-replica-cluster}
  FROM LOAD GENERATOR COUNTER (MAX CARDINALITY 0)

> SELECT count(*) FROM counter3
0

> SELECT mz_unsafe.mz_sleep(1)
<null>

> SELECT count(*) FROM counter3
0

# Check that negative tick intervals are rejected
! CREATE SOURCE counter4
  IN CLUSTER ${arg.single-replica-cluster}
  FROM LOAD GENERATOR COUNTER (TICK INTERVAL '-1s')
contains:invalid TICK INTERVAL: cannot convert negative interval to duration

# Check that out of range tick interval values are rejected
! CREATE SOURCE counter5
  IN CLUSTER ${arg.single-replica-cluster}
  FROM LOAD GENERATOR COUNTER (TICK INTERVAL '2147483647d')
contains: out of range integral type conversion

# Query automatically generated progress topic
$ set-regex match=\d+ replacement=<NUMBER>
> SELECT "offset" FROM another.auction_house_progress
<NUMBER>

# Ensure we report the write frontier of the progress subsource
$ set-regex match=(\s{12}0|\d{13,20}|u\d{1,5}|\(\d+-\d\d-\d\d\s\d\d:\d\d:\d\d\.\d\d\d\)|true|false) replacement=<>
> EXPLAIN TIMESTAMP FOR SELECT * FROM another.auction_house_progress
"                query timestamp: <> <>\n          oracle read timestamp: <> <>\nlargest not in advance of upper: <> <>\n                          upper:[<> <>]\n                          since:[<> <>]\n        can respond immediately: <>\n                       timeline: Some(EpochMilliseconds)\n              session wall time: <> <>\n\nsource materialize.another.auction_house_progress (<>, storage):\n                  read frontier:[<> <>]\n                 write frontier:[<> <>]\n"

# Check that for all tables clause is rejected with no subsources
! CREATE SOURCE counter6
  IN CLUSTER ${arg.single-replica-cluster}
  FROM LOAD GENERATOR COUNTER (MAX CARDINALITY 8, TICK INTERVAL '0.001s')
  FOR ALL TABLES;
contains: FOR ALL TABLES is only valid for multi-output sources

> DROP SOURCE auction_house CASCADE
> DROP SOURCE another.auction_house CASCADE
> DROP SOURCE counter CASCADE
> DROP SOURCE counter3 CASCADE
