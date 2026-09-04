# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# One phase of the long-running Unity Catalog sink test: append a batch and
# confirm the sink is still healthy. Run repeatedly with --no-reset, so it must
# not create or drop anything. mzcompose.py checks that the rows reached the
# catalog; this only checks Materialize's own view.

> INSERT INTO src SELECT n, 'row_' || n::text FROM generate_series(${arg.lo}, ${arg.hi}) AS n

> SELECT status, error IS NULL FROM mz_internal.mz_sink_statuses WHERE name = 'databricks_sink'
running true

# The status above is point-in-time, so it would miss a stall that already
# recovered. A credential the sink failed to refresh would show up here.
> SELECT count(*)
  FROM mz_internal.mz_sink_status_history h
  JOIN mz_sinks s ON s.id = h.sink_id
  WHERE s.name = 'databricks_sink' AND h.error IS NOT NULL
0
