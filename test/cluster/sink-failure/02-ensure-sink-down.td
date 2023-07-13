# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> SELECT DISTINCT mz_sinks.name, error
  FROM
      mz_internal.mz_sink_status_history
          JOIN mz_sinks ON sink_id = mz_sinks.id
  WHERE
      mz_sinks.name = 'snk'
          AND
      status = 'stalled';
snk "synthetic error"
