# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Make sure that various network-level errors are handled without panics or other unexpected issues
# The error messages recored herein are the ones currently returned, even if not very informative.
#

#
# Point to invalid host:port
#

! CREATE MATERIALIZED SOURCE foo
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'foo'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY 'http://127.0.0.1:65535'
  ENVELOPE NONE
contains:connect error: Connection refused

#
# Point to invalid URL schema
#

! CREATE MATERIALIZED SOURCE foo
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'foo'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY 'ftp://127.0.0.1'
  ENVELOPE NONE
contains:URL scheme is not allowed

#
# Point to a valid HTTP/S URL that is not a registry service
#

! CREATE MATERIALIZED SOURCE foo
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'foo'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}/no-such-path'
  ENVELOPE NONE
contains:fetching latest schema for subject 'foo-value' from registry: subject not found
