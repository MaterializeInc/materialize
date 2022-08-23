# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Note these are nonsense schemas, as this error happens before schema verification of any form
$ set keyschema={}

$ set schema={}

$ kafka-create-topic topic=headers_src

! CREATE SOURCE headers_src
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC
  'testdrive-headers_src-${testdrive.seed}'
  KEY FORMAT AVRO USING SCHEMA '${keyschema}'
  VALUE FORMAT AVRO USING SCHEMA '${schema}'
  INCLUDE HEADERS
  ENVELOPE DEBEZIUM
contains:INCLUDE HEADERS requires ENVELOPE UPSERT or no ENVELOPE

! CREATE SOURCE headers_src
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC
  'testdrive-headers_src-${testdrive.seed}'
  KEY FORMAT AVRO USING SCHEMA '${keyschema}'
  VALUE FORMAT AVRO USING SCHEMA '${schema}'
  INCLUDE HEADERS
  ENVELOPE DEBEZIUM UPSERT
contains:INCLUDE HEADERS requires ENVELOPE UPSERT or no ENVELOPE

! CREATE SOURCE headers_src
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC
  'testdrive-headers_src-${testdrive.seed}'
  KEY FORMAT AVRO USING SCHEMA '${keyschema}'
  VALUE FORMAT AVRO USING SCHEMA '${schema}'
  INCLUDE HEADERS
  ENVELOPE MATERIALIZE
contains:INCLUDE HEADERS requires ENVELOPE UPSERT or no ENVELOPE

! CREATE SOURCE headers_src
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC
  'testdrive-headers_src-${testdrive.seed}'
  KEY FORMAT AVRO USING SCHEMA '${keyschema}'
  VALUE FORMAT AVRO USING SCHEMA '${schema}'
  INCLUDE HEADERS
  ENVELOPE MATERIALIZE
contains:INCLUDE HEADERS requires ENVELOPE UPSERT or no ENVELOPE

# even the csv header validation doesn't happen before this error
$ s3-create-bucket bucket=test
$ s3-put-object bucket=test key=static.csv

> CREATE SECRET s3_conn_secret_access_key AS '${testdrive.aws-secret-access-key}';

> CREATE CONNECTION s3_conn FOR AWS
  ACCESS KEY ID = '${testdrive.aws-access-key-id}',
  SECRET ACCESS KEY = SECRET s3_conn_secret_access_key,
  TOKEN = '${testdrive.aws-token}';

! CREATE SOURCE headers_src
  FROM S3 CONNECTION s3_conn
  DISCOVER OBJECTS MATCHING 'static.csv' USING BUCKET SCAN 'testdrive-test-${testdrive.seed}'
  WITH (
    region = '${testdrive.aws-region}',
    endpoint = '${testdrive.aws-endpoint}'
  )
  FORMAT CSV WITH HEADER (city, state, zip)
  INCLUDE HEADERS
contains:INCLUDE HEADERS with non-Kafka sources not supported
