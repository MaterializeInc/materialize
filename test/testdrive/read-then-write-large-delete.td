# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Deleting a few GB of rows should not drop the connection between
# environmentd and the replica.
#
# For a DELETE, the replica sends every deleted row to environmentd in one
# message. It builds the whole message before sending any of it, and sends
# nothing else meanwhile. If that takes longer than environmentd's timeout,
# environmentd drops the connection and reconnects.
#
# The DELETE still succeeds after a few reconnects, so the test checks for the
# reconnects instead.

# 10 chunks x 300k rows x 2 KB = about 6 GB.
$ set-arg-default chunk-rows=300000
$ set-arg-default row-bytes=2000

# The DELETE takes over a minute.
$ set-sql-timeout duration=900s
> SET statement_timeout = '10min'

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET grpc_client_http2_keep_alive_timeout = '2s'
ALTER SYSTEM SET max_result_size = '100GB'
ALTER SYSTEM SET compute_prometheus_introspection_scrape_interval = '1s'

# The 2s timeout (the lowest allowed) means 6 GB is enough to trigger the bug.
# A replica keeps the timeout it had when it was created, so only this new
# cluster gets 2s. `quickstart` keeps the default.
> CREATE CLUSTER rtw_large SIZE 'scale=1,workers=1'

# Load the table on `quickstart`. An INSERT ... SELECT sends its rows the same
# way a DELETE does, so it would also trigger the bug on `rtw_large`.
# environmentd holds all of an INSERT's rows in memory, so load in chunks.

> CREATE TABLE t (id int, payload text)

> INSERT INTO t SELECT 0 * ${arg.chunk-rows} + id, repeat('x', ${arg.row-bytes}) FROM generate_series(1, ${arg.chunk-rows}) AS id
> INSERT INTO t SELECT 1 * ${arg.chunk-rows} + id, repeat('x', ${arg.row-bytes}) FROM generate_series(1, ${arg.chunk-rows}) AS id
> INSERT INTO t SELECT 2 * ${arg.chunk-rows} + id, repeat('x', ${arg.row-bytes}) FROM generate_series(1, ${arg.chunk-rows}) AS id
> INSERT INTO t SELECT 3 * ${arg.chunk-rows} + id, repeat('x', ${arg.row-bytes}) FROM generate_series(1, ${arg.chunk-rows}) AS id
> INSERT INTO t SELECT 4 * ${arg.chunk-rows} + id, repeat('x', ${arg.row-bytes}) FROM generate_series(1, ${arg.chunk-rows}) AS id
> INSERT INTO t SELECT 5 * ${arg.chunk-rows} + id, repeat('x', ${arg.row-bytes}) FROM generate_series(1, ${arg.chunk-rows}) AS id
> INSERT INTO t SELECT 6 * ${arg.chunk-rows} + id, repeat('x', ${arg.row-bytes}) FROM generate_series(1, ${arg.chunk-rows}) AS id
> INSERT INTO t SELECT 7 * ${arg.chunk-rows} + id, repeat('x', ${arg.row-bytes}) FROM generate_series(1, ${arg.chunk-rows}) AS id
> INSERT INTO t SELECT 8 * ${arg.chunk-rows} + id, repeat('x', ${arg.row-bytes}) FROM generate_series(1, ${arg.chunk-rows}) AS id
> INSERT INTO t SELECT 9 * ${arg.chunk-rows} + id, repeat('x', ${arg.row-bytes}) FROM generate_series(1, ${arg.chunk-rows}) AS id

# Delete everything on the 2s cluster.
> SET cluster = rtw_large

> DELETE FROM t

> SELECT count(*) FROM t
0

# Now check whether `rtw_large` reconnected. The replica's metrics reach SQL
# a second or two late, so first wait until they show a message that
# arrived after the DELETE.

$ set-from-sql var=deleted-at
SELECT extract(epoch FROM now())::int8::text

> SELECT max(value) >= ${deleted-at}
  FROM mz_introspection.mz_cluster_prometheus_metrics
  WHERE metric_name = 'mz_cluster_server_last_command_received'
    AND labels -> 'server_name' = 'compute'
true

# Every reconnect adds `mz_compute_reconciliation_*` rows, so any rows mean
# the connection dropped. The rows never go away, so a short retry is enough.
$ set-sql-timeout duration=5s

> SELECT count(*)
  FROM mz_introspection.mz_cluster_prometheus_metrics
  WHERE metric_name LIKE 'mz_compute_reconciliation_%'
0
