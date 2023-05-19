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

$ set-from-sql var=source_id
SELECT id FROM mz_sources WHERE name = 'failpoint'

$ set-regex match="\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d(\.\d\d\d)?" replacement="<TIMESTAMP>"

> select * from mz_internal.mz_source_statuses where id = '${source_id}';
"${source_id}" failpoint kafka "<TIMESTAMP> UTC" running <null> <null>

> SET failpoints = '${arg.failpoint}=${arg.action}';

$ kafka-ingest format=bytes topic=failpoint key-format=bytes key-terminator=:
mammal:whale

> select * from mz_internal.mz_source_statuses where id = '${source_id}';
"${source_id}" failpoint kafka "<TIMESTAMP> UTC" stalled <null> <null>
