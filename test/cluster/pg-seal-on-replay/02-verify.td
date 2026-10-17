# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# The replay finds the schema change at an LSN below the upper `t_before`
# resumed from. The error still has to reach the shard that the seal closes.

! SELECT * FROM t_before
contains:incompatible schema change

> SELECT write_frontier IS NULL
  FROM mz_internal.mz_frontiers
  JOIN mz_tables ON id = object_id
  WHERE name = 't_before'
true

# The other hydrated table keeps ingesting.
$ postgres-execute connection=postgres://postgres:postgres@postgres
INSERT INTO t_alongside VALUES (40);

> SELECT * FROM t_alongside
10
20
30
40
