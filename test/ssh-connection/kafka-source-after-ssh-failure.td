# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# This test script runs after the SSH bastion host has been terminated.

# Ensure that the source status reflects the error before the source restarts.
# This error comes from the Kafka source's metadata fetch loop.
> SELECT status FROM mz_internal.mz_source_statuses st
  JOIN mz_sources s ON st.id = s.id
  WHERE s.name = 'mz_source' AND error LIKE '%Could not resolve hostname ssh-bastion-host: Name or service not known%'
stalled
> SELECT status FROM mz_internal.mz_source_statuses st
  JOIN mz_sources s ON st.id = s.id
  WHERE s.name = 'csr_source' AND error LIKE '%Could not resolve hostname ssh-bastion-host: Name or service not known%'
stalled

# Drop and recreate the source cluster's replica so we can test behavior after
# a restart.
> DROP CLUSTER REPLICA sc.r1;
> CREATE CLUSTER REPLICA sc.r1 SIZE '1';

# Verify that the, after restart, the source reports that the SSH tunnel was
# unable to connect.
> SELECT status FROM mz_internal.mz_source_statuses st
  JOIN mz_sources s ON st.id = s.id
  WHERE s.name = 'mz_source' AND error LIKE '%failed creating kafka consumer: creating ssh tunnel: ssh tunnel: failed to connect to the remote host%'
stalled
