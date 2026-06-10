# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-sql-timeout duration=60s

$ set-arg-default single-replica-cluster=quickstart

# Test the PARTITION BY option for Kafka sinks

> CREATE CONNECTION k
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT)

> CREATE TABLE input (a int, b int);

# Test that `PARTITION BY` does not work with an invalid data type.

! CREATE SINK bad
  IN CLUSTER ${arg.single-replica-cluster}
  FROM input
  INTO KAFKA CONNECTION k (
    TOPIC 'testdrive-bad-${testdrive.seed}',
    PARTITION BY '2024-01-01'::date
  )
  KEY (a) NOT ENFORCED
  FORMAT JSON ENVELOPE UPSERT
contains:PARTITION BY does not support casting from date to uint8

# Test that `PARTITION BY` does not work with invalid column references.

! CREATE SINK bad
  IN CLUSTER ${arg.single-replica-cluster}
  FROM input
  INTO KAFKA CONNECTION k (
    TOPIC 'testdrive-bad-${testdrive.seed}',
    PARTITION BY noexist
  )
  KEY (a) NOT ENFORCED
  FORMAT JSON ENVELOPE UPSERT
contains:column "noexist" does not exist

! CREATE SINK bad
  IN CLUSTER ${arg.single-replica-cluster}
  FROM input
  INTO KAFKA CONNECTION k (
    TOPIC 'testdrive-bad-${testdrive.seed}',
    PARTITION BY b
  )
  KEY (a) NOT ENFORCED
  FORMAT JSON ENVELOPE DEBEZIUM
contains:PARTITION BY expression cannot refer to non-key column "b"

# Test that `PARTITION BY` works for direct partition assignment.

> DROP TABLE input CASCADE
> CREATE MATERIALIZED VIEW input (part, value) AS
  VALUES (0, 'apple'), (1, 'banana'), (2, 'grape'), (3, 'orange'), (0, 'zucchini')

> CREATE SINK direct_output
  IN CLUSTER ${arg.single-replica-cluster}
  FROM input
  INTO KAFKA CONNECTION k (
    TOPIC 'testdrive-direct-${testdrive.seed}',
    TOPIC PARTITION COUNT 2,
    PARTITION BY part
  )
  KEY (value) NOT ENFORCED
  FORMAT JSON
  ENVELOPE UPSERT

$ kafka-verify-data format=json sink=materialize.public.direct_output sort-messages=true
{"part": 0, "value": "apple"} partition=0
{"part": 0, "value": "zucchini"} partition=0
{"part": 1, "value": "banana"} partition=1
{"part": 2, "value": "grape"} partition=0
{"part": 3, "value": "orange"} partition=1

# Test that `PARTITION BY` works with the standard kafka_murmur2 hash function.

> DROP MATERIALIZED VIEW input CASCADE
> CREATE TABLE input (value text)
> INSERT INTO input VALUES ('apple'), ('banana'), ('grape'), ('orange'), ('zucchini')

> CREATE SINK hashed_output
  IN CLUSTER ${arg.single-replica-cluster}
  FROM input
  INTO KAFKA CONNECTION k (
    TOPIC 'testdrive-hashed-${testdrive.seed}',
    TOPIC PARTITION COUNT 16,
    PARTITION BY kafka_murmur2(value)
  )
  KEY (value) NOT ENFORCED
  KEY FORMAT TEXT
  VALUE FORMAT JSON
  ENVELOPE UPSERT

# These partition assignments were verified to match kcat's asssignments
# when using the `murmur2_random` partitioner.
#
# Data was produced via:
#
#     $ kcat -b localhost:9092 -t test-partitioning -P -K : -X topic.partitioner=murmur2_random <<EOF
#     apple:val
#     grape:val
#     zucchini:val
#     banana:val
#     orange:val
#     EOF
#
# And then partition assignments were read back via:
#
#     $ kcat -b localhost:9092 -C -t test-partitioning -f '%k %p\n'
#
$ kafka-verify-data format=json sink=materialize.public.hashed_output sort-messages=true
{"value": "apple"} partition=5
{"value": "banana"} partition=13
{"value": "grape"} partition=5
{"value": "orange"} partition=8
{"value": "zucchini"} partition=4

> DELETE FROM input WHERE value IN ('banana', 'orange')

$ kafka-verify-data key-format=text value-format=json sink=materialize.public.hashed_output sort-messages=true
"banana" "<null>" partition=13
"orange" "<null>" partition=8

# Test that `PARTITION BY` sends errors and invalid values to partition 0.

> DROP TABLE input CASCADE
> CREATE MATERIALIZED VIEW input (a, b) AS
  VALUES (2::int, 1::int), (-1, 1), (1, 0), (1, 1)

> CREATE SINK invalid_output
  IN CLUSTER ${arg.single-replica-cluster}
  FROM input
  INTO KAFKA CONNECTION k (
    TOPIC 'testdrive-invalid-${testdrive.seed}',
    TOPIC PARTITION COUNT 4,
    PARTITION BY a / b
  )
  KEY (a) NOT ENFORCED
  FORMAT JSON
  ENVELOPE UPSERT

$ kafka-verify-data format=json sink=materialize.public.invalid_output sort-messages=true
{"a": -1, "b": 1} partition=0
{"a": 1, "b": 0}  partition=0
{"a": 1, "b": 1}  partition=1
{"a": 2, "b": 1}  partition=2

# Test that null values in PARTITION BY expressions go to partition 0.

> DROP MATERIALIZED VIEW input CASCADE
> CREATE TABLE input (part int, value text);

> INSERT INTO input VALUES (0, 'zero'), (1, 'one'), (null, 'null-value-1'), (2, 'two'), (null, 'null-value-2')

> CREATE SINK null_partition_output
  IN CLUSTER ${arg.single-replica-cluster}
  FROM input
  INTO KAFKA CONNECTION k (
    TOPIC 'testdrive-null-partition-${testdrive.seed}',
    TOPIC PARTITION COUNT 4,
    PARTITION BY part
  )
  KEY (value) NOT ENFORCED
  FORMAT JSON
  ENVELOPE UPSERT

$ kafka-verify-data format=json sink=materialize.public.null_partition_output sort-messages=true
{"part": null, "value": "null-value-1"} partition=0
{"part": null, "value": "null-value-2"} partition=0
{"part": 0, "value": "zero"} partition=0
{"part": 1, "value": "one"} partition=1
{"part": 2, "value": "two"} partition=2

# Test that `PARTITION BY` works with `ENVELOPE DEBEZIUM`.

> DROP TABLE input CASCADE
> CREATE TABLE input (k int, v text);

> CREATE SINK debezium_output
  IN CLUSTER ${arg.single-replica-cluster}
  FROM input
  INTO KAFKA CONNECTION k (
    TOPIC 'testdrive-debezium-${testdrive.seed}',
    TOPIC PARTITION COUNT 2,
    PARTITION BY k
  )
  KEY (k) NOT ENFORCED
  FORMAT JSON
  ENVELOPE DEBEZIUM

> INSERT INTO input VALUES (0, 'apple'), (1, 'banana');

$ kafka-verify-data format=json sink=materialize.public.debezium_output sort-messages=true
{"before": null, "after": {"k": 0, "v": "apple"}} partition=0
{"before": null, "after": {"k": 1, "v": "banana"}} partition=1

> UPDATE input SET v = v || 's'

$ kafka-verify-data format=json sink=materialize.public.debezium_output sort-messages=true
{"before": {"k": 0, "v": "apple"}, "after": {"k": 0, "v": "apples"}} partition=0
{"before": {"k": 1, "v": "banana"}, "after": {"k": 1, "v": "bananas"}} partition=1

> DELETE FROM input

$ kafka-verify-data format=json sink=materialize.public.debezium_output sort-messages=true
{"before": {"k": 0, "v": "apples"}, "after": null} partition=0
{"before": {"k": 1, "v": "bananas"}, "after": null} partition=1
