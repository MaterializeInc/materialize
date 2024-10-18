# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> SELECT name, status, error
  FROM
      mz_internal.mz_source_statuses
  WHERE
      name = ANY (ARRAY['mz_source', 't1', 'ten']);
mz_source stalled "postgres: recoverable errors should crash the process during snapshots"
t1 stalled "postgres: recoverable errors should crash the process during snapshots"
ten stalled "postgres: recoverable errors should crash the process during snapshots"
