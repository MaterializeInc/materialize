# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# join scenario: two persist sources joined on a key. A raw `Join`'s
# `implementation` is `Unimplemented` and the LIR lowering rejects it, so the
# dataflow uses `optimize` to run the MIR optimizer, which picks a join
# implementation before lowering. Without the flag the dataflow would not lower.
create-instance
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

write-rows shard=left schema=kv ts=0
  1 a
  2 b
  3 c
----
wrote 3

write-rows shard=right schema=kv ts=0
  1 x
  2 y
  2 z
----
wrote 3

# Join the two sources on their key column — `#0` (left key) equals `#2` (right key)
# in the 4-column output — and arrange the result. `optimize` is what lets the
# `Join` lower.
create-dataflow name=join as-of=0 optimize
  import source=1000 shard=left schema=kv upper=1
  import source=1001 shard=right schema=kv upper=1
  build id=2000
    Join on=(#0 = #2)
      Get u1000
      Get u1001
  export index=2001 on=2000 key=[0]
----
ok

schedule id=2001
----
ok

await-frontier id=2001 ts=1
----
ok

# k=1 matches one right row, k=2 matches two, k=3 none — three joined rows.
count id=2001 ts=0
----
3
