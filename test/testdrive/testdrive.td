# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Tests for testdrive itself.

# Uncomment to test that timeouts happen in the time desired.
#
# Note that the duration string format can be anything parsable
# by the parse_duration create
#
# $ set-sql-timeout duration=2minutes
# $ set-sql-timeout duration=default
# > select * from nonexistent

# Test that hashing rows works and is consistent.

> CREATE VIEW v AS VALUES (1, 'foo'), (2, 'bar'), (3, 'foo'), (1, 'bar')

> SELECT * FROM v
4 values hashing to 7dd470c8470b085df13552e191a244ab

> VALUES ('row', 1), ('row', 2)
row 1
# inline comment
row 2

# Test DATE , TIME, TIMESTAMP output

> CREATE TABLE t1 (f1 DATE, f2 TIME, f3 TIMESTAMP)

> INSERT INTO t1 VALUES ('2011-11-11', '11:11:11', '2011-11-11 11:11:11')

> SELECT * FROM t1
"2011-11-11" "11:11:11" "2011-11-11 11:11:11"

# Test set-regex

$ set-regex match=u\d+ replacement=UID

> EXPLAIN SELECT * FROM t1;
"%0 =\n| Get materialize.public.t1 (UID)\n"

! SELECT * FROM u1234;
unknown catalog item 'UID'

# Exclude FETCH from the retry logic

> CREATE MATERIALIZED VIEW v1 AS VALUES (1),(2),(3);

> BEGIN

> DECLARE c CURSOR FOR TAIL v1 AS OF 18446744073709551615;

> FETCH 4 c WITH (timeout='10s');
18446744073709551615 1 1
18446744073709551615 1 2
18446744073709551615 1 3

> COMMIT

# kafka-verify sort-messages

> CREATE VIEW sort_messages (a) AS VALUES (2),(1),(3);

> CREATE SINK sort_messages_sink FROM sort_messages
  INTO KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'sort-messages-sink'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'

$ kafka-verify format=avro sink=materialize.public.sort_messages_sink sort-messages=true
{"before": null, "after": {"row": {"a": 1}}}
{"before": null, "after": {"row": {"a": 2}}}
{"before": null, "after": {"row": {"a": 3}}}

# Use $ postgresql-execute and ${testdrive.materialize_addr}

$ postgres-execute connection=postgres://materialize:materialize@${testdrive.materialized-addr}
CREATE TABLE postgres_execute (f1 INTEGER);
INSERT INTO postgres_execute VALUES (123);

> SELECT * FROM postgres_execute;
123
