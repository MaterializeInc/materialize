# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ kafka-create-topic topic=autospill

> CREATE CONNECTION conn
  FOR KAFKA BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT

> CREATE SOURCE autospill
  IN CLUSTER storage_cluster
  FROM KAFKA CONNECTION conn (TOPIC
  'testdrive-autospill-${testdrive.seed}'
  )

> CREATE TABLE autospill_tbl FROM SOURCE autospill (REFERENCE "testdrive-autospill-${testdrive.seed}")
  KEY FORMAT TEXT VALUE FORMAT TEXT
  ENVELOPE UPSERT


$ kafka-ingest format=bytes topic=autospill key-format=bytes key-terminator=:
bird:goose
animal:whale

> SELECT count(*) from autospill_tbl;
2

> SELECT
  SUM(u.bytes_indexed) > 0
  FROM mz_tables t
  JOIN mz_internal.mz_source_statistics_raw u ON t.id = u.id
  WHERE t.name = 'autospill_tbl'
true
