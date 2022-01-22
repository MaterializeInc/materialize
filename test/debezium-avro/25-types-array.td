# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# ARRAY columns are not supported
#

$ postgres-execute connection=postgres://postgres:postgres@postgres
CREATE TABLE array_type (f1 integer[3][3]);
INSERT INTO array_type VALUES ('{{1,2,3},{4,5,6},{7,8,9}}');

! CREATE MATERIALIZED SOURCE array_type
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'postgres.public.array_type'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE DEBEZIUM;
contains:Unsupported type in schema
