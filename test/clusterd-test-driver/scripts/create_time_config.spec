# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# create-time config scenario: supply a create-time dyncfg snapshot with
# `create-instance`, then build and serve an index over it. The snapshot rides
# in `InstanceConfig` and is applied to the replica's worker config before
# create-time setup, ahead of the first `update-configuration`.
#
# This is a plumbing check: it exercises the create-time snapshot end to end and
# asserts the instance still creates dataflows and serves peeks. The snapshot's
# values are non-functional (they do not change query results), so the count is
# the same as without a snapshot.
create-instance
enable_column_paged_batcher bool true
----
ok

update-configuration
----
ok

initialization-complete
----
ok

write-single-ts shard=data ts=0 count=5000
----
wrote 5000

define-index source=1000 index=1001 shard=data key=[0] as-of=0 upper=1
----
ok

schedule id=1001
----
ok

await-frontier id=1001 ts=1
----
ok

count id=1001 ts=0
----
5000
