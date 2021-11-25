# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# a test that failes because the ALL_PROXY env var is incorrect

! CREATE SOURCE invalid_proxy
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-doesntmatter-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE DEBEZIUM
error trying to connect: tcp connect error: Cannot assign requested address (os error 99)

! CREATE MATERIALIZED SOURCE s3_all
  FROM S3 DISCOVER OBJECTS USING BUCKET SCAN 'doesnt-matter'
  WITH (
    region = '${testdrive.aws-region}',
    endpoint = '${testdrive.aws-endpoint}',
    access_key_id = '${testdrive.aws-access-key-id}',
    secret_access_key = '${testdrive.aws-secret-access-key}',
    token = '${testdrive.aws-token}'
  )
  FORMAT TEXT;
Cannot assign requested address (os error 99)
