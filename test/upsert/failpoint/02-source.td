# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ kafka-create-topic topic=failpoint

> CREATE CONNECTION conn
  FOR KAFKA BROKER '${testdrive.kafka-addr}'

> CREATE SOURCE failpoint
  IN CLUSTER storage_cluster
  FROM KAFKA CONNECTION conn (TOPIC
  'testdrive-failpoint-${testdrive.seed}'
  )
  KEY FORMAT TEXT VALUE FORMAT TEXT
  ENVELOPE UPSERT

$ kafka-ingest format=bytes topic=failpoint key-format=bytes key-terminator=:
fish:fish
bird:goose

> SELECT * from failpoint
key           text
-------------------
fish          fish
bird          goose

> select st.name, st.type, st.status, st.error
  from mz_internal.mz_source_statuses st
  join mz_sources s on s.id = st.id
  where s.name = 'failpoint';
failpoint kafka running <null>
