# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# index_and_mv scenario: a single dataflow that exports both an index and a
# materialized view over the same built object (binding u2000). The index serves
# peeks from the replica's arrangement; the MV sink writes the same collection to a
# target shard, read back via a persist peek of the sink id. Both peeks return the
# same rows, demonstrating that one dataflow can carry multiple exports of distinct
# kinds over a shared binding.
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

# One dataflow, two exports over the same view binding (u2000). The build's body is
# an mz-expr-parser MIR spec; `Project (#0, #1)` keeps both columns, so the MV's
# output schema matches the source schema.
create-dataflow name=index-and-mv as-of=0
  import source=1000 shard=r upper=1
  build id=2000
    Project (#0, #1)
      Get u1000
  export kind=index index=2001 on=2000 key=[0]
  export kind=materialized-view sink=2002 on=2000 shard=mv
----
ok

# Schedule both exports of the dataflow.
schedule id=2001
----
ok

schedule id=2002
----
ok

# The MV sink starts read-only; allow it to write to its target shard.
allow-writes id=2002
----
ok

# Peek the index: rows served from the replica's arrangement.
peek id=2001 ts=0
----
1 "alpha"
2 "beta"
3 "gamma"

# Peek the MV sink id: the same rows, read back from the output shard via a persist
# peek (the `SELECT * FROM mv` path).
peek id=2002 ts=0
----
1 "alpha"
2 "beta"
3 "gamma"
