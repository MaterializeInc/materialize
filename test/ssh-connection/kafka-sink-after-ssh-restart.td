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
