# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# reduce scenario: assert a computed result by running a reduce as the
# dataflow-under-test, rather than counting a peek in the driver. Load and index
# a shard, then `define` a `count(*)` reduce over the index and `peek` its
# single-row output. This is the pattern `count` is sugar for.
#
# Load 3000 rows at ts 0 and hydrate an index keyed by column 0.
create-instance
----
ok

update-configuration
----
ok

initialization-complete
----
ok

write-single-ts shard=r ts=0 count=3000
----
wrote 3000

define-index source=1000 index=1001 shard=r key=[0] as-of=0 upper=1
----
ok

schedule id=1001
----
ok

await-frontier id=1001 ts=1
----
ok

# A one-column schema for the count reduce's output (a single bigint).
define-schema name=count_out
  count bigint
----
ok

# Define a dataflow that imports the index and reduces it to a global count(*),
# exporting the result as a peekable index. The build's body is an mz-expr-parser
# MIR spec; `Get u1000` is the collection the index arranges.
create-dataflow name=count as-of=0
  import index=1001
  build id=2000
    Reduce aggregates=[count(*)]
      Get u1000
  export index=2001 on=2000 key=[0]
----
ok

schedule id=2001
----
ok

await-frontier id=2001 ts=1
----
ok

# The reduce output is exactly one row carrying the count.
peek id=2001 schema=count_out ts=0
----
3000
