# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET storage_statistics_collection_interval = 1000
ALTER SYSTEM SET storage_statistics_interval = 2000

> CREATE CLUSTER webhook_cluster REPLICAS (r1 (SIZE '1'));

> CREATE SOURCE webhook_text IN CLUSTER webhook_cluster FROM WEBHOOK
  BODY FORMAT TEXT;

$ webhook-append database=materialize schema=public name=webhook_text
a

> SELECT
    s.name,
    u.messages_received,
    u.bytes_received > 0,
    u.updates_staged,
    u.updates_committed
  FROM mz_sources s
  JOIN mz_internal.mz_source_statistics u ON s.id = u.id
  WHERE s.name IN ('webhook_text')
webhook_text 1 true 1 1

$ set-from-sql var=bytes-received
SELECT
    (u.bytes_received)::text
  FROM mz_sources s
  JOIN mz_internal.mz_source_statistics u ON s.id = u.id
  WHERE s.name IN ('webhook_text')

$ webhook-append database=materialize schema=public name=webhook_text
b

> SELECT
    s.name,
    u.messages_received,
    u.bytes_received > ${bytes-received},
    u.updates_staged,
    u.updates_committed
  FROM mz_sources s
  JOIN mz_internal.mz_source_statistics u ON s.id = u.id
  WHERE s.name IN ('webhook_text')
webhook_text 2 true 2 2

> DROP SOURCE webhook_text CASCADE;

> SELECT
    count(*)
  FROM mz_sources s
  JOIN mz_internal.mz_source_statistics u ON s.id = u.id
  WHERE s.name IN ('webhook_text')
0
