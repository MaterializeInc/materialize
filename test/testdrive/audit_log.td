# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test expected population of mz_audit_events after some DDL statements.

$ set-regex match=u\d+ replacement=<GID>

$ kafka-create-topic topic=test

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}');

> CREATE SOURCE kafka_src
  FROM KAFKA CONNECTION kafka_conn
  (TOPIC 'testdrive-test-${testdrive.seed}')
  FORMAT CSV WITH 2 COLUMNS

> SELECT event_type, object_type, details, user FROM mz_audit_events ORDER BY id DESC LIMIT 1
create  source  "{\"database\":\"materialize\",\"id\":\"<GID>\",\"item\":\"kafka_src\",\"schema\":\"public\",\"size\":\"${arg.default-storage-size}\",\"type\":\"kafka\"}"  materialize

$ s3-create-bucket bucket=test

> CREATE SECRET s3_conn_secret_access_key AS '${testdrive.aws-secret-access-key}';

> CREATE CONNECTION s3_conn TO AWS (
    ACCESS KEY ID = '${testdrive.aws-access-key-id}',
    SECRET ACCESS KEY = SECRET s3_conn_secret_access_key,
    TOKEN = '${testdrive.aws-token}',
    REGION = '${testdrive.aws-region}',
    ENDPOINT = '${testdrive.aws-endpoint}'
  );

> CREATE SOURCE s3_src
  FROM S3 CONNECTION s3_conn
  DISCOVER OBJECTS MATCHING 'posix' USING BUCKET SCAN 'testdrive-test-${testdrive.seed}'
  FORMAT BYTES;

> SELECT event_type, object_type, details, user FROM mz_audit_events ORDER BY id DESC LIMIT 1
create  source  "{\"database\":\"materialize\",\"id\":\"<GID>\",\"item\":\"s3_src\",\"schema\":\"public\",\"size\":\"${arg.default-storage-size}\",\"type\":\"s3\"}"  materialize
