# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# index scenario: load at ts 0 and hydrate, then tick the frontier forward with
# incremental appends at advancing timestamps, and assert the final count.
# No `schema`, so the built-in (bigint, text) sample schema is used.
#
# Load phase: 5000 rows at ts 0, index keyed by column 0, hydrate to ts 1.
create-instance
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

# Tick phase: 5 batches of 1000 rows at ts 1..5. Each uses a disjoint id range
# (start = 5000 + (t-1)*1000) so rows never consolidate and the count accumulates.
write-single-ts shard=data ts=1 start=5000 count=1000
----
wrote 1000

await-frontier id=1001 ts=2
----
ok

write-single-ts shard=data ts=2 start=6000 count=1000
----
wrote 1000

await-frontier id=1001 ts=3
----
ok

write-single-ts shard=data ts=3 start=7000 count=1000
----
wrote 1000

await-frontier id=1001 ts=4
----
ok

write-single-ts shard=data ts=4 start=8000 count=1000
----
wrote 1000

await-frontier id=1001 ts=5
----
ok

write-single-ts shard=data ts=5 start=9000 count=1000
----
wrote 1000

await-frontier id=1001 ts=6
----
ok

# 5000 loaded + 5 * 1000 ticked = 10000.
count id=1001 ts=5
----
10000
