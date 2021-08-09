# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# This test verifies that SELECTs on unmaterialized sources have the
# same error regardless of if there's an AS OF clause or not.

$ set schema={
    "type": "record",
    "name": "envelope",
    "fields": []
  }

> CREATE SOURCE s
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-${testdrive.seed}'
  FORMAT AVRO USING SCHEMA '${schema}'

! SELECT * FROM s AS OF 0
Unable to automatically determine a timestamp for your query; this can happen if your query depends on non-materialized sources.
