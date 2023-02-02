# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ kafka-create-topic topic=topic

$ kafka-ingest format=bytes topic=topic
one
two
three

> CREATE CLUSTER clusterd REPLICAS (
    r1 (
      STORAGECTL ADDRESSES ['clusterd:2100'],
      STORAGE ADDRESSES ['clusterd:2103'],
      COMPUTECTL ADDRESSES ['clusterd:2101'],
      COMPUTE ADDRESSES ['clusterd:2102'],
      WORKERS 4
    )
  )

> CREATE CONNECTION conn TO KAFKA (BROKER '${testdrive.kafka-addr}')

> CREATE SOURCE topic
  IN CLUSTER clusterd
  FROM KAFKA CONNECTION conn (
    TOPIC 'testdrive-topic-${testdrive.seed}'
  )
  FORMAT BYTES
  INCLUDE OFFSET
  ENVELOPE NONE
