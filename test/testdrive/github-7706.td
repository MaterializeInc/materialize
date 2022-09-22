# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# This test verifies that SELECT with AS OF on unmaterialized sources
# does not panic.



$ set schema={
    "type": "record",
    "name": "envelope",
    "fields": []
  }

> CREATE CONNECTION kafka_conn
  FOR KAFKA BROKER '${testdrive.kafka-addr}';

> CREATE SOURCE s
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-data-${testdrive.seed}')
  FORMAT AVRO USING SCHEMA '${schema}'

! SELECT * FROM s AS OF 0
contains:Timestamp (0) is not valid for all inputs

# Although not the original issue, also test that AS OF does work on an
# unmaterialized source if the timestamp is queryable.
> SELECT * FROM s AS OF AT LEAST 0
