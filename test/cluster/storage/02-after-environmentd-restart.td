# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Verify that the data ingested before `environmentd` was killed is still
# present, then try ingesting more data.

# Increased from the default because of CI flakiness.
$ set-sql-timeout duration=180s

> SELECT * from remote1
one
> SELECT * from remote2
one

# ensure after envd has restarted, we have maintained statistics.
> SELECT s.name,
  SUM(u.updates_committed) > 0,
  SUM(u.messages_received),
  SUM(u.offset_known),
  SUM(u.offset_committed)
  FROM mz_sources s
  JOIN mz_internal.mz_source_statistics_raw u ON s.id = u.id
  WHERE s.name IN ('remote1', 'remote2')
  GROUP BY s.id, s.name
remote1 true 1 1 1
remote2 true 1 1 1

$ kafka-ingest format=bytes topic=remote1
two
$ kafka-ingest format=bytes topic=remote2
two

> SELECT * from remote1
one
two
> SELECT * from remote2
one
two

# Ensure that offsets/counters can be updated correctly.
> SELECT s.name,
  SUM(u.updates_committed) > 0,
  SUM(u.messages_received),
  SUM(u.offset_known),
  SUM(u.offset_committed)
  FROM mz_sources s
  JOIN mz_internal.mz_source_statistics_raw u ON s.id = u.id
  WHERE s.name IN ('remote1', 'remote2')
  GROUP BY s.id, s.name
remote1 true 2 2 2
remote2 true 2 2 2
