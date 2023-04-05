# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Ensure that the source becomes healthy again once the SSH tunnel is restarted.
# We specifically make sure that new data written to the Kafka topic is visible
# in the source, as that is the true measure of health, vs what is reported in
# the mz_source_statuses relation.

$ kafka-ingest topic=thetopic format=bytes
three

> SELECT * FROM mz_source
text
----
one
two
three

> SELECT status FROM mz_internal.mz_source_statuses st
  JOIN mz_sources s ON st.id = s.id
  WHERE s.name = 'mz_source'
running

> SELECT status FROM mz_internal.mz_source_statuses st
  JOIN mz_sources s ON st.id = s.id
  WHERE s.name = 'csr_source'
running
