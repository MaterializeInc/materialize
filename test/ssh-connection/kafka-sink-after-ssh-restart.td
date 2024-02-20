# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Ensure that the sinks become healthy again.

# ensure they all were marked as running correctly
> SELECT status FROM mz_internal.mz_sink_statuses st
  JOIN mz_sinks s ON st.id = s.id
  WHERE s.name in ('sink_fixed', 'sink_dynamic')
running
running

# Ensure we can maintain statistics after teh sink restarts
> SELECT s.name, SUM(u.messages_staged), SUM(u.messages_committed), SUM(u.bytes_staged) > 0, SUM(bytes_staged) = SUM(bytes_committed)
  FROM mz_sinks s
  JOIN mz_internal.mz_sink_statistics_raw u ON s.id = u.id
  WHERE s.name IN ('sink_fixed')
  GROUP BY s.name
sink_fixed 2 2 true true
