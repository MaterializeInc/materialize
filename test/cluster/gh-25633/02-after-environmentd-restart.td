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

> SELECT
    (u.rehydration_latency)::text = '${arg.rehydration-latency}'
  FROM mz_sources s
  JOIN mz_internal.mz_source_statistics u ON s.id = u.id
  WHERE s.name IN ('count')
true

> DROP SOURCE IF EXISTS count CASCADE
> DROP CLUSTER IF EXISTS test_cluster CASCADE
