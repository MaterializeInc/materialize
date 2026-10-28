# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# With the broker gone, the sink restarts once its metadata fetches have
# failed for kafka_metadata_failure_timeout, which the status history records.
$ set-sql-timeout duration=120s

> SELECT count(*) > 0
  FROM mz_internal.mz_sink_status_history h
  JOIN mz_sinks s ON h.sink_id = s.id
  WHERE s.name = 'restart_sink'
  AND h.error LIKE '%kafka metadata fetches have failed%'
true
