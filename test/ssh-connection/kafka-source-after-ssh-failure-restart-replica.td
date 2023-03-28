# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# This test script runs after the SSH bastion host has been terminated.

> DROP CLUSTER REPLICA sc.r1
> CREATE CLUSTER REPLICA sc.r1 SIZE '32'

# TODO(guswynn): these should be correctly namespaced as ssh errors
> SELECT status FROM mz_internal.mz_source_statuses st
  JOIN mz_sources s ON st.id = s.id
  WHERE error LIKE 'kafka:%'
  AND s.name in ('fixed_text')
stalled

> SELECT status FROM mz_internal.mz_source_statuses st
  JOIN mz_sources s ON st.id = s.id
  WHERE error LIKE 'decode:%'
  AND s.name in ('fixed_plus_csr', 'dynamic_plus_csr')
stalled
stalled

# top-level ssh sources that don't use ssh in decoding work correctly already
> SELECT status FROM mz_internal.mz_source_statuses st
  JOIN mz_sources s ON st.id = s.id
  WHERE error LIKE 'ssh:%'
  AND s.name in ('dynamic_text')
stalled
