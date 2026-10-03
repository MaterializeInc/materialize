# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# two_runtime_query_dataflow scenario: a query dataflow on the interactive runtime
# that starts before the index it imports has produced anything.
#
# Unlike two_runtime_index.spec, which peeks a maintenance index directly, this
# builds a `count(*)` reduce over a maintenance index and exports it under a
# transient id with `until = as_of + 1`, which makes it a one-shot read that the
# interactive runtime renders.
#
# The maintenance index is created but not scheduled, so its publisher has sealed
# nothing when the interactive dataflow is created and scheduled over it. Only then
# is the index scheduled, which fills the publication and wakes the import. The
# result peek then returns the correct count.
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

# The maintenance index over shard `r`, created but not yet scheduled.
create-dataflow name=maint-index as-of=0
  import source=1000 shard=r upper=1
  build id=2000
    Project (#0, #1)
      Get u1000
  export kind=index index=2001 on=2000 key=[0]
----
ok

# A one-column schema for the count reduce's output (a single bigint).
define-schema name=count_out
  count bigint
----
ok

# The interactive query dataflow: `count(*)` over the maintenance index, exported
# under a transient id and bounded one step past `as_of`, so the interactive
# runtime renders it.
create-dataflow name=interactive-count as-of=0 single-read
  import index=2001
  build id=3000
    Reduce aggregates=[count(*)]
      Get u2000
  export index=t4000 on=3000 key=[0]
----
ok

# Scheduled before the maintenance index, so the import starts over a publication
# that has sealed nothing.
schedule id=t4000
----
ok

# Scheduling the index releases the publisher, which fills the publication and
# wakes the import.
schedule id=2001
----
ok

peek id=t4000 schema=count_out ts=0
----
3
