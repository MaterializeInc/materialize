# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# This test script runs after the SSH bastion host has been terminated.
$ set-sql-timeout duration=300s

# Ensure they all are marked as broken for ssh reasons. Sinks continuously restart, so we
# instead check the history
> SELECT s.name, count(*) > 0 FROM mz_internal.mz_sink_status_history st
  JOIN mz_sinks s ON st.sink_id = s.id
  WHERE error LIKE 'ssh:%'
  AND s.name in ('sink_fixed', 'sink_dynamic')
  GROUP BY s.name
sink_fixed true
sink_dynamic true

$ kafka-ingest topic=thetopic format=bytes
one

# Ensure we maintain statistics even if the sink is broken.
> SELECT s.name, SUM(u.messages_staged), SUM(u.messages_committed), SUM(u.bytes_staged) > 0, SUM(bytes_staged) = SUM(bytes_committed)
  FROM mz_sinks s
  JOIN mz_internal.mz_sink_statistics_raw u ON s.id = u.id
  WHERE s.name IN ('sink_fixed')
  GROUP BY s.name
sink_fixed 1 1 true true
