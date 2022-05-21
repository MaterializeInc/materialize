# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

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

$ kafka-ingest format=avro topic=headers_src key-format=avro key-schema=${keyschema} schema=${schema} headers={"gus": "gusfive", "gus2": "gus3"}
{"key": "fish"} {"f1": "fishval", "f2": 1000}

$ kafka-ingest format=avro topic=headers_src key-format=avro key-schema=${keyschema} schema=${schema}
{"key": "fish2"} {"f1": "fishval", "f2": 1000}


> CREATE MATERIALIZED SOURCE headers_src
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC
  'testdrive-headers_src-${testdrive.seed}'
  KEY FORMAT AVRO USING SCHEMA '${keyschema}'
  VALUE FORMAT AVRO USING SCHEMA '${schema}'
  INCLUDE HEADERS
  ENVELOPE UPSERT

# empty case + has headers case
> SELECT key, f1, f2, list_length(headers), headers::text from headers_src
key     f1       f2     list_length    headers
----------------------------------------------
fish    fishval  1000   2              "{\"(gus,\\\\x67757366697665)\",\"(gus2,\\\\x67757333)\"}"
fish2   fishval  1000   0              "{}"

# unpacking works
> SELECT key, f1, f2, headers[1].value as gus from headers_src
key     f1       f2     gus
-------------------------------------------
fish    fishval  1000   gusfive
fish2   fishval  1000   <null>

# selecting by key works
> SELECT key, f1, f2, thekey, value FROM (SELECT i.key, i.f1, i.f2, unnest(headers).key as thekey, unnest(headers).value as value from headers_src as I) i WHERE thekey = 'gus'
key     f1       f2     thekey  value
-------------------------------------------
fish    fishval  1000   gus     gusfive


# The headers dict is entirely overwritten, even if the value AND the remaining header hasn't changed
$ kafka-ingest format=avro topic=headers_src key-format=avro key-schema=${keyschema} schema=${schema} headers={"gus":"gusfive"}
{"key": "fish"} {"f1": "fishval", "f2": 1000}

# empty case + has headers case
> SELECT key, f1, f2, list_length(headers) from headers_src
key     f1       f2     list_length
-------------------------------------------
fish    fishval  1000   1
fish2   fishval  1000   0

# Headers with the same key are both preserved
$ kafka-ingest format=avro topic=headers_src key-format=avro key-schema=${keyschema} schema=${schema} headers=[{"gus": "a"}, {"gus": "b"}]
{"key": "fish"} {"f1": "fishval", "f2": 1000}

> SELECT key, f1, f2, headers[1].value as gus1, headers[2].value as gus2 from headers_src
key     f1       f2     gus1     gus2
-------------------------------------------
fish    fishval  1000   a        b
fish2   fishval  1000   <null>   <null>


# Works with other includes
$ kafka-create-topic topic=headers_also partitions=1

$ kafka-ingest format=avro topic=headers_also key-format=avro key-schema=${keyschema} schema=${schema} headers={"gus":"gusfive"}
{"key": "fish"} {"f1": "fishval", "f2": 1000}

> CREATE MATERIALIZED SOURCE headers_also
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC
  'testdrive-headers_also-${testdrive.seed}'
  KEY FORMAT AVRO USING SCHEMA '${keyschema}'
  VALUE FORMAT AVRO USING SCHEMA '${schema}'
  INCLUDE HEADERS, PARTITION
  ENVELOPE UPSERT

> SELECT key, f1, f2, list_length(headers), partition from headers_also
key     f1       f2     list_length    partition
-----------------------------------------------
fish    fishval  1000   1             0

# esoteric ingestions
$ kafka-ingest format=avro topic=headers_also key-format=avro key-schema=${keyschema} schema=${schema} headers={"gus": "gus=five"}
{"key": "fish"} {"f1": "fishval", "f2": 1000}

> SELECT key, f1, f2, headers[1].value as gus, partition from headers_also
key     f1       f2     gus           partition
-----------------------------------------------
fish    fishval  1000   gus=five      0

# null header
$ kafka-ingest format=avro topic=headers_also key-format=avro key-schema=${keyschema} schema=${schema} headers={"gus": null}
{"key": "fish"} {"f1": "fishval", "f2": 1000}

> SELECT key, f1, f2, headers[1].value as gus, partition from headers_also
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

! CREATE MATERIALIZED SOURCE headers_conflict
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC
  'testdrive-headers_conflict-${testdrive.seed}'
  KEY FORMAT AVRO USING SCHEMA '${keyschema}'
  VALUE FORMAT AVRO USING SCHEMA '${schemaheaders}'
  INCLUDE HEADERS
  ENVELOPE UPSERT
contains: column "headers" specified more than once

# No meaningful way to get data out in td because of the ambiguous name
# + weird type
# > SELECT * from headers_conflict


$ kafka-ingest format=avro topic=headers_conflict key-format=avro key-schema=${keyschema} schema=${schemaheaders} headers={"gus": "gusfive"}
{"key": "fish"} {"headers": "value"}

# using AS to resolve it!
> CREATE MATERIALIZED SOURCE headers_conflict2
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC
  'testdrive-headers_conflict-${testdrive.seed}'
  KEY FORMAT AVRO USING SCHEMA '${keyschema}'
  VALUE FORMAT AVRO USING SCHEMA '${schemaheaders}'
  INCLUDE HEADERS AS kafka_headers
  ENVELOPE UPSERT

> SELECT key, headers, kafka_headers[1].value as gus from headers_conflict2
key     headers  gus
------------------------
fish    value    gusfive
