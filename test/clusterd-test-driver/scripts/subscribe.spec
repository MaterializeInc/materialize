# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# subscribe scenario: a sink streaming changes back as responses (no shard). Write
# explicit rows at two timestamps, subscribe to the source, and assert the streamed
# updates. Subscribe responses arrive asynchronously, so the driver buffers them;
# `await-subscribe` waits for the sink's upper to reach `up-to`, then renders the
# consolidated `<ts> <diff> <datums>` updates (sorted for determinism).
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

# Exports with different owners cannot share a dataflow. Reject the plan rather
# than silently splitting it or installing a transient sink on the native owner.
create-dataflow as-of=0
  import source=1000 shard=s schema=kv upper=2
  build id=3000
    Get u1000
  export kind=index index=3001 on=3000 key=[0]
  export kind=subscribe sink=t3002 on=3000 schema=kv up-to=2
----
error: a dataflow cannot mix maintained and transient exports

# Import the source and subscribe to it. `up-to=2` makes the subscribe complete
# once it has streamed everything in `[0, 2)`.
create-dataflow name=sub as-of=0
  import source=1000 shard=s schema=kv upper=2
  build id=2000
    Get u1000
  export kind=subscribe sink=t2001 on=2000 schema=kv up-to=2
  export kind=index index=t2002 on=2000 key=[0]
----
ok

schedule id=t2001
----
ok

# Both exports share the dataflow start token.
schedule id=t2002
----
ok

# The snapshot lands at ts 0; the ts-1 insert streams as its own update.
await-subscribe id=t2001 up-to=2
----
0 1 1 "alpha"
0 1 2 "beta"
1 1 3 "gamma"


# A transient index and subscription can share one query-owned plan. Counting
# the index must build its reduce in that same query scope, not on the owner.
count id=t2002 ts=1
----
3
