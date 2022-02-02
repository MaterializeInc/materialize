# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Reading all that data after starting up can take longer than the default timeout.
$ set-sql-timeout duration=60s

> SELECT COUNT(*) FROM failpoint;
1000000

# We expect to read at least some of the messages from Kafka after restarting
> SELECT SUM(CAST(statistics->'topics'->'testdrive-failpoint-${testdrive.seed}'->'partitions'->'0'->'msgs' AS INT)) > 1 FROM mz_kafka_source_statistics;
true

# We also expect, though, that some messages we're successfully read before we activated the failpoint and restarted
> SELECT SUM(CAST(statistics->'topics'->'testdrive-failpoint-${testdrive.seed}'->'partitions'->'0'->'msgs' AS INT)) < 1000000 FROM mz_kafka_source_statistics;
true
