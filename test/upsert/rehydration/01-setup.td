# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> DROP CLUSTER IF EXISTS storage_cluster CASCADE;

# Using mem-2 because it has defined memory limits which will be passed for
# backpressure inflight bytes configuration
> CREATE CLUSTER storage_cluster SIZE 'mem-2', REPLICATION FACTOR 1
