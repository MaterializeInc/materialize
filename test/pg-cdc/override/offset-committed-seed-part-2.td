# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Run while the workflow holds an ACCESS EXCLUSIVE lock on t2's primary key
# index and keeps inserting into t1: adding t2 re-renders the source, and t2's
# snapshot blocks on the lock.

$ set-sql-timeout duration=60s

> CREATE TABLE t2 FROM SOURCE mz_source (REFERENCE t2);

> SELECT bool_and(NOT u.snapshot_committed)
  FROM mz_tables t
  JOIN mz_internal.mz_source_statistics u ON t.id = u.id
  WHERE t.name = 't2'
true
