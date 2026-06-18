# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# materialized_view scenario: a persist sink writing a computed collection to a
# target shard (a materialized view). Load and count a shard, sink the count to a
# new shard, then `peek` the sink id — which the driver serves as a persist peek of
# the output shard, the same path `SELECT * FROM mv` takes. The persist peek blocks
# until the sink seals through the timestamp, so no separate read-back is needed.
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

# A one-column schema for the count materialized view's output (a single bigint).
define-schema name=count_out
  count bigint
----
ok

# Import the source, reduce it to a global count(*), and sink that to shard `mv`.
# The build's body is an mz-expr-parser MIR spec; `Get u1000` is the source.
create-dataflow name=count-mv as-of=0
  import source=1000 shard=r upper=1
  build id=2000
    Reduce aggregates=[count(*)]
      Get u1000
  export kind=materialized-view sink=2001 on=2000 shard=mv schema=count_out
----
ok

schedule id=2001
----
ok

# Every dataflow starts read-only; a persist sink withholds writes until AllowWrites.
allow-writes id=2001
----
ok

# Peeking the sink id reads its output shard via a persist peek: one row, the count.
peek id=2001 schema=count_out ts=0
----
3000
