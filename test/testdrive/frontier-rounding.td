# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Tests checking that frontiers of storage collections, at least when they
# don't receive data updates, are rounded to seconds. This is important because
# it ensures that all dataflow sources tick forward at the same time, reducing
# the number of distinct times dataflows have to process, thus removing some
# overhead.
#
# Note that we expect frontiers to be of the form XX..XX001, i.e. rounded to
# seconds and then stepped forward. That's because things that tick forward
# collections choose XX..XX000 as the append timestamp, and then the new write
# frontier must be greater than that.
#
# The rounding is currently not working correctly for tables, compute
# introspection sources, storage-managed collections. See database-issues#9030.

$ kafka-create-topic topic=topic partitions=1

> CREATE CONNECTION kafka_conn
  TO KAFKA (BROKER '${testdrive.kafka-addr}', SECURITY PROTOCOL PLAINTEXT)

> CREATE SOURCE src
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-topic-${testdrive.seed}')
> CREATE TABLE src_tbl FROM SOURCE src
> CREATE SOURCE ctr FROM LOAD GENERATOR COUNTER
> CREATE TABLE ctr_tbl FROM SOURCE ctr
> CREATE SOURCE actn FROM LOAD GENERATOR AUCTION
> CREATE TABLE actn_accounts FROM SOURCE actn (REFERENCE accounts)

# sources
> SELECT DISTINCT write_frontier::text::uint8 % 1000 = 1
  FROM mz_internal.mz_frontiers
  JOIN mz_sources ON id = object_id
  WHERE object_id LIKE 'u%'
true

# tables
# fails: frontiers are not rounded
# > SELECT DISTINCT write_frontier::text::uint8 % 1000 = 1
#   FROM mz_internal.mz_frontiers
#   JOIN mz_tables ON id = object_id
# true

# compute introspection sources
# fails: frontiers are rounded to `XX..XX000` instead of `XX..XX001`
# > SELECT DISTINCT write_frontier::text::uint8 % 1000 = 1
#   FROM mz_internal.mz_frontiers
#   WHERE object_id LIKE 'si%'
# true

# storage-managed collections
# fails: frontiers are not rounded
# > SELECT DISTINCT write_frontier::text::uint8 % 1000 = 1
#   FROM mz_internal.mz_frontiers
#   JOIN mz_sources ON id = object_id
#   WHERE object_id LIKE 's%'
# true
