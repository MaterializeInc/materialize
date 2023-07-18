# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> CREATE SOURCE auction_house FROM LOAD GENERATOR AUCTION FOR ALL TABLES;

> SHOW SOURCES
accounts                subsource      <null>
auction_house           load-generator ${arg.default-storage-size}
auction_house_progress  progress       <null>
auctions                subsource      <null>
bids                    subsource      <null>
organizations           subsource      <null>
users                   subsource      <null>

# Adding test to check for tables() work
> CREATE SCHEMA a;
> CREATE SOURCE a.auction_bids FROM LOAD GENERATOR AUCTION FOR TABLES (bids);

> SHOW SOURCES FROM a;
auction_bids            load-generator ${arg.default-storage-size}
auction_bids_progress   progress       <null>
bids                    subsource      <null>

# For Tables with mentioned schema should work
> CREATE SCHEMA another;
> CREATE SOURCE another.auction_house FROM LOAD GENERATOR AUCTION FOR ALL TABLES;

> SHOW SOURCES FROM another;
accounts                subsource      <null>
auction_house           load-generator ${arg.default-storage-size}
auction_house_progress  progress       <null>
auctions                subsource      <null>
bids                    subsource      <null>
organizations           subsource      <null>
users                   subsource      <null>

# Subsources should be created in source schema if no schema is provided
> CREATE SCHEMA foo;
> CREATE SCHEMA bar;
> CREATE SOURCE foo.auction_subset
  FROM LOAD GENERATOR AUCTION
  FOR TABLES (bids as foo_bids, users as bar.bar_users);

> SHOW SOURCES FROM foo;
auction_subset            load-generator ${arg.default-storage-size}
auction_subset_progress   progress       <null>
foo_bids                  subsource      <null>

> SHOW SOURCES FROM bar;
bar_users                 subsource      <null>

> CREATE CONNECTION IF NOT EXISTS kafka_conn TO KAFKA (BROKER '${testdrive.kafka-addr}');

# Validate that the ID column of the load generator data is usable as a key.
> CREATE SINK accounts_sink FROM accounts
  INTO KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-accounts-${testdrive.seed}')
  KEY (id)
  FORMAT JSON
  ENVELOPE UPSERT;

# Show that AST of subsource contains REFERENCES option
> SHOW CREATE SOURCE accounts
materialize.public.accounts "CREATE SUBSOURCE \"materialize\".\"public\".\"accounts\" (\"id\" \"pg_catalog\".\"int8\" NOT NULL, \"org_id\" \"pg_catalog\".\"int8\" NOT NULL, \"balance\" \"pg_catalog\".\"int8\" NOT NULL, UNIQUE (\"id\")) WITH (REFERENCES = true)"

# Check that non-append-only `COUNTER` sources reach the proper size

> CREATE SOURCE counter FROM LOAD GENERATOR COUNTER (MAX CARDINALITY 8, TICK INTERVAL '0.001s') WITH (SIZE '1')

> SELECT count(*) FROM counter
8

# Now make sure it doesn't change
> SELECT mz_internal.mz_sleep(1)
<null>

> SELECT count(*) FROM counter
8

# Check that negative max cardinalities are rejected
! CREATE SOURCE counter2 FROM LOAD GENERATOR COUNTER (MAX CARDINALITY -1) WITH (SIZE '1')
contains: unsupported max cardinality

> CREATE SOURCE counter3 FROM LOAD GENERATOR COUNTER (MAX CARDINALITY 0) WITH (SIZE '1')

> SELECT count(*) FROM counter3
0

> SELECT mz_internal.mz_sleep(1)
<null>

> SELECT count(*) FROM counter3
0

# Check that negative tick intervals are rejected
! CREATE SOURCE counter4 FROM LOAD GENERATOR COUNTER (TICK INTERVAL '-1s')
contains: out of range integral type conversion

# Check that out of range tick interval values are rejected
! CREATE SOURCE counter5 FROM LOAD GENERATOR COUNTER (TICK INTERVAL '2147483647d')
contains: out of range integral type conversion

# Query automatically generated progress topic
$ set-regex match=\d+ replacement=<NUMBER>
> SELECT "offset" FROM auction_house_progress
<NUMBER>

# Ensure we report the write frontier of the progress subsource
$ set-regex match=(\s{12}0|\d{13,20}|u\d{1,5}|\(\d+-\d\d-\d\d\s\d\d:\d\d:\d\d.\d\d\d\)|true|false) replacement=<>
> EXPLAIN TIMESTAMP FOR SELECT * FROM auction_house_progress
"                query timestamp: <> <>\n          oracle read timestamp: <> <>\nlargest not in advance of upper: <> <>\n                          upper:[<> <>]\n                          since:[<> <>]\n        can respond immediately: <>\n                       timeline: Some(EpochMilliseconds)\n              session wall time: <> <>\n\nsource materialize.public.auction_house_progress (<>, storage):\n                  read frontier:[<> <>]\n                 write frontier:[<> <>]\n"

# Check that for all tables clause is rejected with no subsources
! CREATE SOURCE counter6
  FROM LOAD GENERATOR COUNTER (MAX CARDINALITY 8, TICK INTERVAL '0.001s')
  FOR ALL TABLES;
contains: FOR ALL TABLES is only valid for multi-output sources

# Check that for tables() clause is rejected with no subsources
! CREATE SOURCE counter6
  FROM LOAD GENERATOR COUNTER (MAX CARDINALITY 8, TICK INTERVAL '0.001s')
  FOR TABLES(t1);
contains: FOR TABLES (..) is only valid for multi-output sources
