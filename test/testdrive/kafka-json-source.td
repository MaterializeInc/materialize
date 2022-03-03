# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> CREATE TYPE test_ints AS (a int, b bigint);

$ kafka-create-topic topic=kafka_ints partitions=1

$ kafka-ingest format=bytes topic=kafka_ints timestamp=1
{"a": 13, "b": 37}

! CREATE MATERIALIZED SOURCE json_source_ints
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-kafka_ints-${testdrive.seed}'
  FORMAT JSON USING TYPE unknown_type;
exact: unknown catalog item 'unknown_type'

> CREATE MATERIALIZED SOURCE json_source_ints
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-kafka_ints-${testdrive.seed}'
  FORMAT JSON USING TYPE test_ints;

> SHOW CREATE SOURCE json_source_ints
Source   "Create Source"
------------------------
materialize.public.json_source_ints "CREATE SOURCE \"materialize\".\"public\".\"json_source_ints\" FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-kafka_ints-${testdrive.seed}' FORMAT JSON USING TYPE \"test_ints\""

> SELECT a, b FROM json_source_ints;
a   b
------
13  37

$ kafka-ingest format=bytes topic=kafka_ints timestamp=1
{"a": 100, "b": 200}

> SELECT a, b FROM json_source_ints;
a   b
------
13  37
100 200

# message with extra columns that get ignored
$ kafka-ingest format=bytes topic=kafka_ints timestamp=1
{"a": 300, "b": 400, "c": "extra text", "d": {"e": 10}}

> SELECT a, b FROM json_source_ints;
a   b
------
13  37
100 200
300 400

# malformed message with missing required column
$ kafka-ingest format=bytes topic=kafka_ints timestamp=1
{"a": 400}

> SELECT a, b FROM json_source_ints;
a   b
------
13  37
100 200
300 400

# how should we handle records whose values don't fit in the types?
# $ kafka-ingest format=bytes topic=kafka_ints timestamp=1
# {"a": 3000000000000000000000000000000000000000000000, "b": 4000000000000000000000000000000000000000000000000, "c": "extra text", "d": {"e": 10}}

# > SELECT a, b FROM json_source_ints;
# a   b
# ------
# 13  37
# 100 200
# 300 400

# note: modifiers on types aren't yet supported in custom types, so we can't test length limits on varchar/character
> CREATE TYPE test_text AS (a varchar, b character, c text);

$ kafka-create-topic topic=kafka_text partitions=1

$ kafka-ingest format=bytes topic=kafka_text timestamp=1
{"a": "abcd", "b": "abcd", "c": "abcd"}

> CREATE MATERIALIZED SOURCE json_source_text
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-kafka_text-${testdrive.seed}'
  FORMAT JSON USING TYPE test_text;

> SELECT a, b, c FROM json_source_text;
"abcd" "a" "abcd"

# TODO(phemberger): test all the other basic data types

> CREATE TYPE test_arrays AS (a bigint[]);

$ kafka-create-topic topic=kafka_arrays partitions=1

$ kafka-ingest format=bytes topic=kafka_arrays timestamp=1
{"a": [1, 2, 3, 4, 5, 6]}

> CREATE MATERIALIZED SOURCE json_source_arrays
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-kafka_arrays-${testdrive.seed}'
  FORMAT JSON USING TYPE test_arrays;

> SELECT a FROM json_source_arrays;
{1,2,3,4,5,6}

# Test out nested Record types

> CREATE TYPE test_inner_record AS (a int[], b text);
> CREATE TYPE test_outer_record AS (inner test_inner_record, c float4);

$ kafka-create-topic topic=kafka_records partitions=1

$ kafka-ingest format=bytes topic=kafka_records timestamp=1
{"c": 1.234, "inner": { "a": [5, 6, 7, 8], "b": "hello, world" } }

> CREATE MATERIALIZED SOURCE json_source_records
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-kafka_records-${testdrive.seed}'
  FORMAT JSON USING TYPE test_outer_record;

> SELECT pg_typeof(inner), pg_typeof(c) FROM json_source_records;
test_inner_record real

> SELECT inner::text, c FROM json_source_records;
"({5,6,7,8},\"hello, world\")" 1.234

> SELECT (inner).b, (inner).a FROM json_source_records;
"hello, world" {5,6,7,8}

# Test out JSONB columns
> CREATE TYPE test_jsonb AS (a int4, b jsonb, c text);

$ kafka-create-topic topic=kafka_jsonb partitions=1

$ kafka-ingest format=bytes topic=kafka_jsonb timestamp=1
{"a": 1111, "b": {"d": "arbitrarily", "e": { "f": "nested" }, "g": ["v", "a", "l", "u", "e", "s"], "h": 2222, "i": 3.456 }, "c": "foo" }

> CREATE MATERIALIZED SOURCE json_source_jsonb
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-kafka_jsonb-${testdrive.seed}'
  FORMAT JSON USING TYPE test_jsonb;

> SELECT pg_typeof(a), pg_typeof(b), pg_typeof(c) FROM json_source_jsonb;
integer jsonb text

> SELECT a,b,c FROM json_source_jsonb;
1111 "{\"d\":\"arbitrarily\",\"e\":{\"f\":\"nested\"},\"g\":[\"v\",\"a\",\"l\",\"u\",\"e\",\"s\"],\"h\":2222,\"i\":3.456}" "foo"

$ kafka-ingest format=bytes topic=kafka_jsonb timestamp=2
{"a": 2222, "b": true, "c": "bar" }
{"a": 3333, "b": ["o", "m", "g"], "c": "baz" }
{"a": 4444, "b": {"b_inner": null}, "c": "foo again"}

> SELECT a,b,c FROM json_source_jsonb;
1111 "{\"d\":\"arbitrarily\",\"e\":{\"f\":\"nested\"},\"g\":[\"v\",\"a\",\"l\",\"u\",\"e\",\"s\"],\"h\":2222,\"i\":3.456}" "foo"
2222 "true" "bar"
3333 "\[\"o\",\"m\",\"g\"\]" "baz"
4444 "{\"b_inner\":null}" "foo again"

# malformed inner document
$ kafka-ingest format=bytes topic=kafka_jsonb timestamp=3
{"a": 2222, "b": {"d": [1,2,3}, "c": "bar" }

! SELECT * from json_source_jsonb;
contains: Decode error: Text: invalid json: expected `,` or `]`

> DROP TYPE test_jsonb;

> DROP TYPE test_outer_record;

> DROP TYPE test_inner_record;

> DROP TYPE test_arrays;

> DROP TYPE test_text;

> DROP TYPE test_ints;
