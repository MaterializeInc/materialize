# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-arg-default default-replica-size=scale=1,workers=1
$ set-arg-default single-replica-cluster=quickstart

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_clock_load_generator = true;

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
  FROM LOAD GENERATOR AUCTION (AS OF 300, UP TO 301);

$ skip-if
SELECT mz_version_num() < 14400;

# Error if trying to create with subsources
! CREATE SOURCE g FROM LOAD GENERATOR COUNTER FOR ALL TABLES;
contains:FOR ALL TABLES is only valid for multi-output sources

! CREATE SOURCE g FROM LOAD GENERATOR CLOCK FOR ALL TABLES;
contains:FOR ALL TABLES is only valid for multi-output sources

! CREATE SOURCE g FROM LOAD GENERATOR DATUMS FOR ALL TABLES;
contains:FOR ALL TABLES is only valid for multi-output sources

! CREATE SOURCE g FROM LOAD GENERATOR COUNTER FOR TABLES ("foo");
regex:.*FOR TABLES.*unsupported

! CREATE SOURCE g FROM LOAD GENERATOR CLOCK FOR TABLES ("foo");
regex:.*FOR TABLES.*unsupported

! CREATE SOURCE g FROM LOAD GENERATOR DATUMS FOR TABLES ("foo");
regex:.*FOR TABLES.*unsupported

! CREATE SOURCE g FROM LOAD GENERATOR COUNTER FOR SCHEMAS ("foo");
regex:.*FOR SCHEMAS.*unsupported

! CREATE SOURCE g FROM LOAD GENERATOR CLOCK FOR SCHEMAS ("foo");
regex:.*FOR SCHEMAS.*unsupported

! CREATE SOURCE g FROM LOAD GENERATOR DATUMS FOR SCHEMAS ("foo");
regex:.*FOR SCHEMAS.*unsupported

# skip-end

> CREATE TABLE accounts FROM SOURCE auction_house (REFERENCE accounts);
> CREATE TABLE auctions FROM SOURCE auction_house (REFERENCE auctions);
> CREATE TABLE bids FROM SOURCE auction_house (REFERENCE bids);
> CREATE TABLE organizations FROM SOURCE auction_house (REFERENCE organizations);
> CREATE TABLE users FROM SOURCE auction_house (REFERENCE users);

> SHOW SOURCES
auction_house           load-generator ${arg.single-replica-cluster}    ""
auction_house_progress  progress       <null>                           ""

> SHOW TABLES
accounts                ""
auctions                ""
bids                    ""
organizations           ""
users                   ""

> SELECT count(*) FROM bids
255

! CREATE SOURCE auction_house
  IN CLUSTER ${arg.single-replica-cluster}
  FROM LOAD GENERATOR AUCTION FOR TABLES (user);
contains:reference to user not found in source

> CREATE SCHEMA another;
> BEGIN
> CREATE SOURCE another.auction_house
  IN CLUSTER ${arg.single-replica-cluster}
  FROM LOAD GENERATOR AUCTION;

> CREATE TABLE another.accounts FROM SOURCE another.auction_house (REFERENCE accounts);
> CREATE TABLE another.auctions FROM SOURCE another.auction_house (REFERENCE auctions);
> CREATE TABLE another.bids FROM SOURCE another.auction_house (REFERENCE bids);
> CREATE TABLE another.organizations FROM SOURCE another.auction_house (REFERENCE organizations);
> CREATE TABLE another.users FROM SOURCE another.auction_house (REFERENCE users);
> COMMIT

> SHOW SOURCES FROM another;
auction_house           load-generator ${arg.single-replica-cluster}    ""
auction_house_progress  progress       <null>                           ""

> SHOW TABLES FROM another;
accounts                ""
auctions                ""
bids                    ""
organizations           ""
users                   ""

> CREATE CONNECTION IF NOT EXISTS kafka_conn TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

# Validate that the ID column of the load generator data is usable as a key.
> CREATE SINK accounts_sink
  IN CLUSTER ${arg.single-replica-cluster}
  FROM accounts
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-accounts-${testdrive.seed}')
  KEY (id)
  FORMAT JSON
  ENVELOPE UPSERT;

$ set-regex match="DETAILS = '[a-f0-9]+'" replacement=<DETAILS>

# CLOCK load generator source

> CREATE SOURCE clock
  IN CLUSTER ${arg.single-replica-cluster}
  FROM LOAD GENERATOR CLOCK (TICK INTERVAL '1s')

> SELECT count(*) FROM clock;
1

> SELECT time < now() + INTERVAL '5s', time > now() - INTERVAL '5s' FROM clock
true true

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
"                query timestamp: <> <>\n          oracle read timestamp: <> <>\nlargest not in advance of upper: <> <>\n                          upper:[<> <>]\n                          since:[<> <>]\n        can respond immediately: <>\n                       timeline: Some(EpochMilliseconds)\n              session wall time: <> <>\n\nsource materialize.another.auction_house_progress (<>, storage):\n                  read frontier:[<> <>]\n                 write frontier:[<> <>]\n\nbinding constraints:\nlower:\n  (IsolationLevel(StrictSerializable)): [<> <>]\n"

> DROP SOURCE auction_house CASCADE
> DROP SOURCE another.auction_house CASCADE
> DROP SOURCE counter CASCADE
> DROP SOURCE counter3 CASCADE
