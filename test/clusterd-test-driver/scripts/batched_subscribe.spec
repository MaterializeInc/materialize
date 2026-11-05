# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# batched-subscribe scenario: a sink reporting changes back as batches of
# descriptions. Description boundaries depend on timing, so
# `await-batched-subscribe` renders the gapless chain the batches form, followed
# by their consolidated `<ts> <diff> <datums>` updates (sorted for determinism).
create-instance
----
ok

update-configuration
----
ok

initialization-complete
----
ok

define-schema name=kv
  k bigint
  v text
----
ok

write-rows shard=s schema=kv ts=0
  1 alpha
  2 beta
----
wrote 2

write-rows shard=s schema=kv ts=1
  3 gamma
----
wrote 1

write-rows shard=s schema=kv ts=2
  4 delta
----
wrote 1

# `up-to=2` completes the chain with the description `[2, empty)`, and no update
# at or after 2 is reported.
create-dataflow name=bounded as-of=0
  import source=1000 shard=s schema=kv upper=3
  build id=2000
    Get u1000
  export kind=batched-subscribe sink=2001 on=2000 schema=kv up-to=2
----
ok

schedule id=2001
----
ok

await-batched-subscribe id=2001
----
chain 0..empty
0 1 1 "alpha"
0 1 2 "beta"
1 1 3 "gamma"

# The first description holds the snapshot at the as-of 1, later ones the updates
# at their own times.
create-dataflow name=unbounded as-of=1
  import source=1100 shard=s schema=kv upper=3
  build id=2100
    Get u1100
  export kind=batched-subscribe sink=2101 on=2100 schema=kv
----
ok

schedule id=2101
----
ok

await-batched-subscribe id=2101 up-to=3
----
chain 1..3
1 1 1 "alpha"
1 1 2 "beta"
1 1 3 "gamma"
2 1 4 "delta"
