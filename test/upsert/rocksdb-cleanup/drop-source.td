# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ kafka-create-topic topic=upsert

> CREATE CONNECTION conn
  FOR KAFKA BROKER '${testdrive.kafka-addr}'

> CREATE SOURCE kept_upsert
  FROM KAFKA CONNECTION conn (TOPIC
  'testdrive-upsert-${testdrive.seed}'
  )
  KEY FORMAT TEXT VALUE FORMAT TEXT
  ENVELOPE UPSERT

> CREATE SOURCE dropped_upsert
  FROM KAFKA CONNECTION conn (TOPIC
  'testdrive-upsert-${testdrive.seed}'
  )
  KEY FORMAT TEXT VALUE FORMAT TEXT
  ENVELOPE UPSERT

$ kafka-ingest format=bytes topic=upsert key-format=bytes key-terminator=:
fish:fish
bird:goose
animal:whale

> SELECT count(*) from dropped_upsert;
3

> SELECT count(*) from kept_upsert;
3
