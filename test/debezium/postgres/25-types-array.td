# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# ARRAY columns are supported
#

$ postgres-execute connection=postgres://postgres:postgres@postgres
CREATE TABLE array_type (f1 text[]);
INSERT INTO array_type VALUES ('{foo, null}');

$ schema-registry-wait-schema schema=postgres.public.array_type-value

> CREATE MATERIALIZED SOURCE array_type
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'postgres.public.array_type'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE DEBEZIUM

# Cast to `text` to work around TD's lack of support for PG arrays
> SELECT f1::text FROM array_type
f1
---
{foo,NULL}
