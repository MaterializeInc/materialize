# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Run after the workflow kills moto, so the Glue backend is unreachable. Both
# Glue-touching planning paths must fail cleanly. Moto-only.

$ set simple-schema-name=mz-test-schema

# Connection validation pings GetRegistry: transport error, not NotFound.
! CREATE CONNECTION glue_conn_unreachable TO AWS GLUE SCHEMA REGISTRY (
    AWS CONNECTION = aws_conn,
    REGISTRY = '${arg.registry-name}'
  )
contains:failed to validate AWS Glue Schema Registry connection

# Table purification pings GetSchemaVersion. The source itself needs no Glue.
> CREATE SOURCE unreachable_source
  IN CLUSTER quickstart
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-simple-${testdrive.seed}')

! CREATE TABLE unreachable_table FROM SOURCE unreachable_source (REFERENCE "testdrive-simple-${testdrive.seed}")
  FORMAT AVRO USING AWS GLUE SCHEMA REGISTRY CONNECTION glue_conn (SCHEMA NAME = '${simple-schema-name}')
  ENVELOPE NONE
contains:Glue schema lookup failed
