# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Verify that the data ingested while `clusterd` was down eventually appears,
# then try ingesting new data.

# Increased from the default because of CI flakiness.
$ set-sql-timeout duration=180s

> SELECT * from remote1
one
two
three
four
> SELECT * from remote2
one
two
three
four

$ kafka-ingest format=bytes topic=remote1
five
$ kafka-ingest format=bytes topic=remote2
five

> SELECT * from remote1
one
two
three
four
five
> SELECT * from remote2
one
two
three
four
five

> SELECT s.name,
  SUM(u.offset_known),
  SUM(u.offset_committed)
  FROM mz_sources s
  JOIN mz_internal.mz_source_statistics u ON s.id = u.id
  WHERE s.name IN ('remote1', 'remote2')
  GROUP BY s.id, s.name
remote1 5 5
remote2 5 5
