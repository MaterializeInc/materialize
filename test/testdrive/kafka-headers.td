# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-arg-default default-storage-size=scale=1,workers=1
$ set-arg-default single-replica-cluster=quickstart

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

$ kafka-create-topic topic=headers_src

# [103, 117, 115, 51] = "gus3"
$ kafka-ingest format=avro topic=headers_src key-format=avro key-schema=${keyschema} schema=${schema} headers={"gus": "gusfive", "gus2": [103, 117, 115, 51]}
{"key": "fish"} {"f1": "fishval", "f2": 1000}

$ kafka-ingest format=avro topic=headers_src key-format=avro key-schema=${keyschema} schema=${schema}
{"key": "fish2"} {"f1": "fishval", "f2": 1000}

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT);

> CREATE CLUSTER headers_src_cluster SIZE '${arg.default-storage-size}';
> CREATE SOURCE headers_src
  IN CLUSTER headers_src_cluster
  FROM KAFKA CONNECTION kafka_conn (TOPIC
  'testdrive-headers_src-${testdrive.seed}')

> CREATE TABLE headers_src_tbl FROM SOURCE headers_src (REFERENCE "testdrive-headers_src-${testdrive.seed}")
  KEY FORMAT AVRO USING SCHEMA '${keyschema}'
  VALUE FORMAT AVRO USING SCHEMA '${schema}'
  INCLUDE HEADERS
  ENVELOPE UPSERT

# empty case + has headers case
> SELECT key, f1, f2, list_length(headers), headers::text from headers_src_tbl
key     f1       f2     list_length    headers
----------------------------------------------
fish    fishval  1000   2              "{\"(gus,\\\"\\\\\\\\x67757366697665\\\")\",\"(gus2,\\\"\\\\\\\\x67757333\\\")\"}"
fish2   fishval  1000   0              "{}"

# unpacking works
> SELECT key, f1, f2, headers[1].value as gus from headers_src_tbl
key     f1       f2     gus
-------------------------------------------
fish    fishval  1000   gusfive
fish2   fishval  1000   <null>

# map_build lets you get the headers as a map
> SELECT key, f1, f2, map_build(headers)->'gus' as gus, map_build(headers)->'gus2' AS gus2 from headers_src_tbl;
key     f1       f2     gus       gus2
-------------------------------------------
fish    fishval  1000   gusfive   gus3
fish2   fishval  1000   <null>    <null>

# selecting by key works
> SELECT key, f1, f2, thekey, value FROM (SELECT i.key, i.f1, i.f2, unnest(headers).key as thekey, unnest(headers).value as value from headers_src_tbl as I) i WHERE thekey = 'gus'
key     f1       f2     thekey  value
-------------------------------------------
fish    fishval  1000   gus     gusfive


# The headers dict is entirely overwritten, even if the value AND the remaining header hasn't changed
$ kafka-ingest format=avro topic=headers_src key-format=avro key-schema=${keyschema} schema=${schema} headers={"gus":"gusfive"}
{"key": "fish"} {"f1": "fishval", "f2": 1000}

# empty case + has headers case
> SELECT key, f1, f2, list_length(headers) from headers_src_tbl
key     f1       f2     list_length
-------------------------------------------
fish    fishval  1000   1
fish2   fishval  1000   0

# Headers with the same key are both preserved
$ kafka-ingest format=avro topic=headers_src key-format=avro key-schema=${keyschema} schema=${schema} headers=[{"gus": "a"}, {"gus": "b"}]
{"key": "fish"} {"f1": "fishval", "f2": 1000}

> SELECT key, f1, f2, headers[1].value as gus1, headers[2].value as gus2 from headers_src_tbl
key     f1       f2     gus1     gus2
-------------------------------------------
fish    fishval  1000   a        b
fish2   fishval  1000   <null>   <null>


# Works with other includes
$ kafka-create-topic topic=headers_also partitions=1

$ kafka-ingest format=avro topic=headers_also key-format=avro key-schema=${keyschema} schema=${schema} headers={"gus":"gusfive"}
{"key": "fish"} {"f1": "fishval", "f2": 1000}

> CREATE CLUSTER headers_also_cluster SIZE '${arg.default-storage-size}';
> CREATE SOURCE headers_also
  IN CLUSTER headers_also_cluster
  FROM KAFKA CONNECTION kafka_conn (TOPIC
  'testdrive-headers_also-${testdrive.seed}')

> CREATE TABLE headers_also_tbl FROM SOURCE headers_also (REFERENCE "testdrive-headers_also-${testdrive.seed}")
  KEY FORMAT AVRO USING SCHEMA '${keyschema}'
  VALUE FORMAT AVRO USING SCHEMA '${schema}'
  INCLUDE HEADERS, PARTITION
  ENVELOPE UPSERT

> SELECT key, f1, f2, list_length(headers), partition from headers_also_tbl
key     f1       f2     list_length    partition
-----------------------------------------------
fish    fishval  1000   1             0

# esoteric ingestions
$ kafka-ingest format=avro topic=headers_also key-format=avro key-schema=${keyschema} schema=${schema} headers={"gus": "gus=five"}
{"key": "fish"} {"f1": "fishval", "f2": 1000}

> SELECT key, f1, f2, headers[1].value as gus, partition from headers_also_tbl
key     f1       f2     gus           partition
-----------------------------------------------
fish    fishval  1000   gus=five      0

# null header
$ kafka-ingest format=avro topic=headers_also key-format=avro key-schema=${keyschema} schema=${schema} headers={"gus": null}
{"key": "fish"} {"f1": "fishval", "f2": 1000}

> SELECT key, f1, f2, headers[1].value as gus, partition from headers_also_tbl
key     f1       f2     gus           partition
-----------------------------------------------
fish    fishval  1000   <null>        0

# conflicting naming
$ set schemaheaders={
        "type" : "record",
        "name" : "test",
        "fields" : [
            {"name":"headers", "type":"string"}
        ]
    }

$ kafka-create-topic topic=headers_conflict

> CREATE SOURCE headers_conflict
  IN CLUSTER ${arg.single-replica-cluster}
  FROM KAFKA CONNECTION kafka_conn (TOPIC
  'testdrive-headers_conflict-${testdrive.seed}')

! CREATE TABLE headers_conflict_tbl
  FROM SOURCE headers_conflict (REFERENCE "testdrive-headers_conflict-${testdrive.seed}")
  KEY FORMAT AVRO USING SCHEMA '${keyschema}'
  VALUE FORMAT AVRO USING SCHEMA '${schemaheaders}'
  INCLUDE HEADERS
  ENVELOPE UPSERT
contains: column "headers" specified more than once

# No meaningful way to get data out in td because of the ambiguous name
# + weird type
# > SELECT * from headers_conflict_tbl


$ kafka-ingest format=avro topic=headers_conflict key-format=avro key-schema=${keyschema} schema=${schemaheaders} headers={"gus": "gusfive"}
{"key": "fish"} {"headers": "value"}

# using AS to resolve it!
> CREATE CLUSTER headers_conflict2_cluster SIZE '${arg.default-storage-size}';
> CREATE SOURCE headers_conflict2
  IN CLUSTER headers_conflict2_cluster
  FROM KAFKA CONNECTION kafka_conn (TOPIC
  'testdrive-headers_conflict-${testdrive.seed}')

> CREATE TABLE headers_conflict2_tbl FROM SOURCE headers_conflict2 (REFERENCE "testdrive-headers_conflict-${testdrive.seed}")
  KEY FORMAT AVRO USING SCHEMA '${keyschema}'
  VALUE FORMAT AVRO USING SCHEMA '${schemaheaders}'
  INCLUDE HEADERS AS kafka_headers
  ENVELOPE UPSERT

> SELECT key, headers, kafka_headers[1].value as gus from headers_conflict2_tbl
key     headers  gus
------------------------
fish    value    gusfive


# test extracting individual headers with INCLUDE HEADER
$ kafka-create-topic topic=individual_headers

$ kafka-ingest format=avro topic=individual_headers key-format=avro key-schema=${keyschema} schema=${schema} headers={"header1": "message_1_header_1"}
{"key": "message_1"} {"f1": "fishval", "f2": 1000}

$ kafka-ingest format=avro topic=individual_headers key-format=avro key-schema=${keyschema} schema=${schema} headers={"header1": "message_2_header_1", "header2": "message_2_header_2"}
{"key": "message_2"} {"f1": "fishval", "f2": 1000}

> CREATE CLUSTER individual_headers_cluster SIZE '${arg.default-storage-size}';
> CREATE SOURCE individual_headers
  IN CLUSTER individual_headers_cluster
  FROM KAFKA CONNECTION kafka_conn (TOPIC
  'testdrive-individual_headers-${testdrive.seed}')

> CREATE TABLE individual_headers_tbl FROM SOURCE individual_headers (REFERENCE "testdrive-individual_headers-${testdrive.seed}")
  KEY FORMAT AVRO USING SCHEMA '${keyschema}'
  VALUE FORMAT AVRO USING SCHEMA '${schema}'
  INCLUDE HEADER 'header1' AS header1
  ENVELOPE UPSERT

> SELECT key, header1 from individual_headers_tbl
key          header1
-------------------------------
message_1    message_1_header_1
message_2    message_2_header_1


# test exposing header as byte array
> CREATE CLUSTER individual_headers_bytes_cluster SIZE '${arg.default-storage-size}';
> CREATE SOURCE individual_headers_bytes
  IN CLUSTER individual_headers_bytes_cluster
  FROM KAFKA CONNECTION kafka_conn (TOPIC
  'testdrive-individual_headers-${testdrive.seed}')

> CREATE TABLE individual_headers_bytes_tbl FROM SOURCE individual_headers_bytes (REFERENCE "testdrive-individual_headers-${testdrive.seed}")
  KEY FORMAT AVRO USING SCHEMA '${keyschema}'
  VALUE FORMAT AVRO USING SCHEMA '${schema}'
  INCLUDE HEADER 'header1' AS header1 BYTES
  ENVELOPE UPSERT

> SELECT key, header1::text from individual_headers_bytes_tbl
key          header1
---------------------------------------------------
message_1    \x6d6573736167655f315f6865616465725f31
message_2    \x6d6573736167655f325f6865616465725f31


# When there are multiple headers with identical keys, verify that the last header is exposed in the row
$ kafka-create-topic topic=duplicate_individual_headers

$ kafka-ingest format=avro topic=duplicate_individual_headers key-format=avro key-schema=${keyschema} schema=${schema} headers=[{"duplicates": "message_3_header_3_first"}, {"duplicates": "message_3_header_3_second"}, {"duplicates": "message_3_header_3_third"}]
{"key": "message_3"} {"f1": "fishval", "f2": 1000}

> CREATE CLUSTER duplicate_individual_headers_cluster SIZE '${arg.default-storage-size}';
> CREATE SOURCE duplicate_individual_headers
  IN CLUSTER duplicate_individual_headers_cluster
  FROM KAFKA CONNECTION kafka_conn (TOPIC
  'testdrive-duplicate_individual_headers-${testdrive.seed}')

> CREATE TABLE duplicate_individual_headers_tbl FROM SOURCE duplicate_individual_headers (REFERENCE "testdrive-duplicate_individual_headers-${testdrive.seed}")
  KEY FORMAT AVRO USING SCHEMA '${keyschema}'
  VALUE FORMAT AVRO USING SCHEMA '${schema}'
  INCLUDE HEADERS, HEADER 'duplicates' AS duplicates
  ENVELOPE UPSERT

> SELECT key, duplicates, headers::text from duplicate_individual_headers_tbl
key          duplicates                 headers
-------------------------------------------------
message_3 message_3_header_3_third "{\"(duplicates,\\\"\\\\\\\\x6d6573736167655f335f6865616465725f335f6669727374\\\")\",\"(duplicates,\\\"\\\\\\\\x6d6573736167655f335f6865616465725f335f7365636f6e64\\\")\",\"(duplicates,\\\"\\\\\\\\x6d6573736167655f335f6865616465725f335f7468697264\\\")\"}"

# We can control the header map more granularly with `map_agg`.
> SELECT
    key,
    map_agg((headers).key, convert_from((headers).value, 'utf-8') ORDER BY (headers).value ASC)::TEXT AS headers_map
  FROM (
    SELECT
      key, unnest(headers) AS headers
    FROM duplicate_individual_headers_tbl
  )
  GROUP BY key;
key       headers_map
---------------------
message_3 "{duplicates=>message_3_header_3_third}"

# Reverse order of aggregating values.
> SELECT
    key,
    map_agg((headers).key, convert_from((headers).value, 'utf-8') ORDER BY (headers).value DESC)::TEXT AS headers_map
  FROM (
    SELECT
      key, unnest(headers) AS headers
    FROM duplicate_individual_headers_tbl
  )
  GROUP BY key;
key       headers_map
---------------------
message_3 "{duplicates=>message_3_header_3_first}"

# Verify that the source is bricked when there are headers that cannot be parsed as utf-8
$ kafka-create-topic topic=ill_formed_header

$ kafka-ingest format=avro topic=ill_formed_header key-format=avro key-schema=${keyschema} schema=${schema} headers={"header1": "message_1_header_1"}
{"key": "message_1"} {"f1": "fishval", "f2": 1000}
$ kafka-ingest format=avro topic=ill_formed_header key-format=avro key-schema=${keyschema} schema=${schema} headers={"header1": "message_2_header_1"}
{"key": "message_2"} {"f1": "fishval", "f2": 1000}

> CREATE CLUSTER ill_formed_header_cluster SIZE '${arg.default-storage-size}';
> CREATE SOURCE ill_formed_header
  IN CLUSTER ill_formed_header_cluster
  FROM KAFKA CONNECTION kafka_conn (TOPIC
  'testdrive-ill_formed_header-${testdrive.seed}')

> CREATE TABLE ill_formed_header_tbl FROM SOURCE ill_formed_header (REFERENCE "testdrive-ill_formed_header-${testdrive.seed}")
  KEY FORMAT AVRO USING SCHEMA '${keyschema}'
  VALUE FORMAT AVRO USING SCHEMA '${schema}'
  INCLUDE HEADERS, HEADER 'header1' AS header1
  ENVELOPE UPSERT

> SELECT key, header1 from ill_formed_header_tbl
key          header1
------------------------------------------
message_1    message_1_header_1
message_2    message_2_header_1

$ kafka-ingest format=avro topic=ill_formed_header key-format=avro key-schema=${keyschema} schema=${schema} headers={"header1": [195, 40]}
{"key": "message_1"} {"f1": "fishval", "f2": 1000}

! SELECT key, header1 from ill_formed_header_tbl
contains:Found ill-formed byte sequence in header 'header1' that cannot be decoded as valid utf-8 (original bytes: [c3, 28])


# Verify that the source is bricked when messages have missing headers

> CREATE CLUSTER missing_headers_cluster SIZE '${arg.default-storage-size}';
> CREATE SOURCE missing_headers
  IN CLUSTER missing_headers_cluster
  FROM KAFKA CONNECTION kafka_conn (TOPIC
  'testdrive-individual_headers-${testdrive.seed}')

> CREATE TABLE missing_headers_tbl FROM SOURCE missing_headers (REFERENCE "testdrive-individual_headers-${testdrive.seed}")
  KEY FORMAT AVRO USING SCHEMA '${keyschema}'
  VALUE FORMAT AVRO USING SCHEMA '${schema}'
  INCLUDE HEADER 'header2' AS header2
  ENVELOPE UPSERT

! SELECT key, header2 from missing_headers_tbl
contains:A header with key 'header2' was not found in the message headers
