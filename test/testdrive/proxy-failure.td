# Copyright Materialize, Inc. All rights reserved.
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
Creating HTTP client for schema registry: builder error: builder error: relative URL without a base
