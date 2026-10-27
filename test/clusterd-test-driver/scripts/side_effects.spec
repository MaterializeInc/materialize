# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# side-effects scenario: drive AllowCompaction explicitly and confirm the replica
# keeps serving. Write at two timestamps so a count lands in [since, upper) after
# compaction: ts 0 (5000 rows) and ts 1 (1000 rows, disjoint ids), sealing upper
# to 2.
create-instance
----
ok

update-configuration
----
ok

initialization-complete
----
ok

write-single-ts shard=se ts=0 count=5000
----
wrote 5000

write-single-ts shard=se ts=1 start=5000 count=1000
----
wrote 1000

define-index source=1000 index=1001 shard=se key=[0] as-of=0 upper=2
----
ok

schedule id=1001
----
ok

await-frontier id=1001 ts=2
----
ok

# Advance the read frontier (since) to ts 1: no consumer will read below 1, so the
# replica may compact data under it.
allow-compaction id=1001 frontier=1
----
ok

# Count at ts 1 (>= since, < upper); both batches are visible: 5000 + 1000.
count id=1001 ts=1
----
6000
