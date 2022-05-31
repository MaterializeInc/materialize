# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test that Kafka sources with no format are disallowed.

! CREATE SOURCE s
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-${testdrive.seed}'
contains:Source format must be specified

> CREATE SECRET sec AS '1234'

> CREATE MATERIALIZED SOURCE secret_kafka FROM
  KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-${testdrive.seed}' WITH (
    security_protocol = 'SSL',
    ssl_key_password = sec
  )
  FORMAT CSV WITH 2 COLUMNS;

> SELECT * FROM secret_kafka;
