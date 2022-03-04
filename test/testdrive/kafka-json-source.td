# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> CREATE TYPE test_simple AS (a int, b bigint, c boolean);

$ kafka-create-topic topic=kafka_simple partitions=1

$ kafka-ingest format=bytes topic=kafka_simple timestamp=1
{"a": 13, "b": 37, "c": false}

! CREATE MATERIALIZED SOURCE json_source_simple
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-kafka_simple-${testdrive.seed}'
  FORMAT JSON USING TYPE unknown_type;
exact: unknown catalog item 'unknown_type'

> CREATE MATERIALIZED SOURCE json_source_simple
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-kafka_simple-${testdrive.seed}'
  FORMAT JSON USING TYPE test_simple;

> SHOW CREATE SOURCE json_source_simple
Source   "Create Source"
------------------------
materialize.public.json_source_simple "CREATE SOURCE \"materialize\".\"public\".\"json_source_simple\" FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-kafka_simple-${testdrive.seed}' FORMAT JSON USING TYPE \"test_simple\""

> SELECT a, b, c FROM json_source_simple;
a   b     c
-------------
13  37  false

$ kafka-ingest format=bytes topic=kafka_simple timestamp=2
{"a": 100, "b": 200, "c": true}

> SELECT a, b, c FROM json_source_simple;
a   b     c
-------------
13  37  false
100 200 true

#
# verify that extra columns are ignored
#

$ kafka-ingest format=bytes topic=kafka_simple timestamp=3
{"a": 300, "b": 400, "c": false, "d": {"e": 10}, "f": null}

> SELECT a, b, c FROM json_source_simple;
a   b     c
-------------
13  37  false
100 200 true
300 400 false

#
# verify handling of explicit JSON null value
#

$ kafka-ingest format=bytes topic=kafka_simple timestamp=4
{"a": 400, "b": 500, "c": null}
{"a": 500, "b": null, "c": true}
{"a": null, "b": 600, "c": false}
{"a": null, "b": null, "c": true}

> SELECT a, b, c FROM json_source_simple ORDER BY a, b ASC;
13     37     false
100    200    true
300    400    false
400    500    <null>
500    <null> true
<null> 600    false
<null> <null> true

#
# verify failure if any fields are missing
#

$ kafka-ingest format=bytes topic=kafka_simple timestamp=4
{"b": 400, "c": true}

! SELECT a, b FROM json_source_simple ORDER BY a ASC;
contains: Decode error: Text: error decoding column a: no value for field a

#
# verify failure if field does not match its type
#

$ kafka-create-topic topic=kafka_range_check partitions=1

> CREATE MATERIALIZED SOURCE json_source_range_check
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-kafka_range_check-${testdrive.seed}'
  FORMAT JSON USING TYPE test_simple;

# try storing 1 larger than an Int32 can fit
$ kafka-ingest format=bytes topic=kafka_range_check timestamp=1
{"a": 2147483648, "b": 2147483648, "c": "extra text", "d": {"e": 10}}

! SELECT a, b FROM json_source_range_check;
contains: error decoding column a: out of range

#
# text fields
#

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

#
# more numeric types
#

$ kafka-create-topic topic=kafka_numerics partitions=1

$ kafka-ingest format=bytes topic=kafka_numerics timestamp=1
{"a": -100, "b": -200, "c": 987654321, "d": 1.23456789, "e": 9.8e-3, "f": 5.1234567898765432123456789}

> CREATE TYPE test_numerics AS (a int2, b int4, c int8, d float4, e double precision, f numeric);

> CREATE MATERIALIZED SOURCE json_source_numerics
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-kafka_numerics-${testdrive.seed}'
  FORMAT JSON USING TYPE test_numerics;

> SELECT a, b, c, d, e, f FROM json_source_numerics;
-100 -200 987654321 1.2345679 0.0098 5.1234567898765432123456789

#
# array types
#

> CREATE TYPE test_arrays AS (a bigint[]);

$ kafka-create-topic topic=kafka_arrays partitions=1

$ kafka-ingest format=bytes topic=kafka_arrays timestamp=1
{"a": [1, 2, 3, 4, 5, 6]}

> CREATE MATERIALIZED SOURCE json_source_arrays
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-kafka_arrays-${testdrive.seed}'
  FORMAT JSON USING TYPE test_arrays;

> SELECT a FROM json_source_arrays;
{1,2,3,4,5,6}

#
# record types for handling embedded JSON objects
#

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

#
# jsonb type
#

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

#
# map, list and int2vector types
#

> CREATE TYPE int4_map AS MAP (key_type = text, value_type = int4);
> CREATE TYPE float8_list AS LIST (element_type = float8);
> CREATE TYPE test_map_and_list AS (a int4_map, b float8_list, c int2vector);

$ kafka-create-topic topic=kafka_map_and_list partitions=1

$ kafka-ingest format=bytes topic=kafka_map_and_list timestamp=1
{"a": {"z_1": 98, "z_2": 76}, "b": [1.23, 3.456, 7.8], "c": [100, 200, 300]}

> CREATE MATERIALIZED SOURCE json_source_map_and_list
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-kafka_map_and_list-${testdrive.seed}'
  FORMAT JSON USING TYPE test_map_and_list;

> SELECT pg_typeof(a), pg_typeof(b), pg_typeof(c) FROM json_source_map_and_list;
int4_map float8_list int2vector

$ kafka-ingest format=bytes topic=kafka_map_and_list timestamp=2
{"a": {"y_1": 54}, "b": [1.234567], "c": []}

> SELECT a::text, b::text, c::text FROM json_source_map_and_list ORDER BY a DESC;
"{z_1=>98,z_2=>76}" "{1.23,3.456,7.8}" "100 200 300"
"{y_1=>54}" "{1.234567}" ""

#
# disallowed field types
#

> CREATE TYPE test_disallowed AS (a regclass);

! CREATE MATERIALIZED SOURCE json_source_ints_three
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-kafka_ints-${testdrive.seed}'
  FORMAT JSON USING TYPE test_disallowed;
contains: unsupported scalar type RegClass in JSON source

> DROP TYPE test_map_and_list;

> DROP TYPE float8_list;

> DROP TYPE int4_map;

> DROP TYPE test_jsonb;

> DROP TYPE test_outer_record;

> DROP TYPE test_inner_record;

> DROP TYPE test_arrays;

> DROP TYPE test_numerics;

> DROP TYPE test_text;

> DROP TYPE test_simple;
