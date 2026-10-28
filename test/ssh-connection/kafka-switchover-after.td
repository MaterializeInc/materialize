# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# The replacement broker carries the same topic with the same offsets, as a
# replicated cluster would.

$ kafka-create-topic topic=switchover

$ kafka-ingest topic=switchover format=bytes
one
two

# Only now let the stalled source rebuild its consumers: a rebuild before the
# topic existed on the replacement broker would be a definite error.
$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET kafka_metadata_failure_timeout = '10s'

$ set-sql-timeout duration=120s

> SELECT status FROM mz_internal.mz_source_statuses st
  JOIN mz_sources s ON st.id = s.id
  WHERE s.name = 'switchover'
running

$ kafka-ingest topic=switchover format=bytes
three

> SELECT * FROM switchover_tbl
text
----
one
two
three
