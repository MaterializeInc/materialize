# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ kafka-create-topic topic=remoted

$ kafka-ingest format=bytes topic=remoted
whatever

> CREATE MATERIALIZED SOURCE remoted
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC
  'testdrive-remoted-${testdrive.seed}'
  FORMAT TEXT
  REMOTE 'storaged:2100'

> SELECT * from remoted
text     mz_offset
------------------
whatever 1
