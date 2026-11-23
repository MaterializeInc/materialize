# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# The broker is down but the SSH tunnel is up, so sources stall with a non-SSH error.
$ set-sql-timeout duration=120s

> SELECT status FROM mz_internal.mz_source_statuses st
  JOIN mz_sources s ON st.id = s.id
  WHERE error NOT LIKE 'ssh:%'
  AND s.name in ('fixed_text', 'dynamic_text', 'fixed_plus_csr', 'dynamic_plus_csr')
stalled
stalled
stalled
stalled
