# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# two_runtime_recreate_index scenario: a reconnect recreates an index at the
# same `as_of`.
#
# The second connection replays the index with another key, so neither runtime
# keeps the first incarnation. Both incarnations have the same id and `as_of`
# but different creation numbers, and the interactive runtime binds the new
# incarnation's peer only to the slot of exactly its id and creation number,
# whether or not the maintenance runtime has dropped the old one yet. The
# first incarnation's view froze at upper 1 when it dropped, so the peek at 1,
# after a write only the second incarnation sees, answers only from the second
# incarnation, and times out on the first.
create-instance
----
ok

update-configuration
----
ok

initialization-complete
----
ok

write-rows shard=r ts=0
  1 alpha
  2 beta
  3 gamma
----
wrote 3

create-dataflow name=first-incarnation as-of=0
  import source=1000 shard=r upper=1
  build id=2000
    Project (#0, #1)
      Get u1000
  export kind=index index=2001 on=2000 key=[0]
----
ok

schedule id=2001
----
ok

peek id=2001 ts=0
----
1 "alpha"
2 "beta"
3 "gamma"

# Drop the connection and reconnect; only `Hello` is sent, so re-open the
# instance to start a new reconciliation window.
reconnect
----
ok

create-instance
----
ok

# Same ids and `as_of`, another key: incompatible with the running dataflow.
create-dataflow name=second-incarnation as-of=0
  import source=1000 shard=r upper=1
  build id=2000
    Project (#0, #1)
      Get u1000
  export kind=index index=2001 on=2000 key=[1]
----
ok

schedule id=2001
----
ok

initialization-complete
----
ok

# The identity peek returns the arrangement's columns, key first.
peek id=2001 ts=0
----
"alpha" 1
"beta" 2
"gamma" 3

write-rows shard=r ts=1
  4 delta
----
wrote 1

peek id=2001 ts=1 timeout-secs=60
----
"alpha" 1
"beta" 2
"delta" 4
"gamma" 3
