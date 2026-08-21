# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# two_runtime_query_dataflow scenario: a query dataflow on the interactive runtime
# whose import binds before the index it imports is published.
#
# Unlike two_runtime_index.spec, which peeks a maintenance index directly, this
# builds a `count(*)` reduce over a maintenance index and exports it under a
# transient id with `until = as_of + 1`, which is what the multiplexer routes to
# the interactive runtime (`DataflowDescription::is_peek_dataflow`).
#
# The maintenance index is registered with `defer`, so the driver knows its shape
# but has not submitted its dataflow. The interactive dataflow is then created and
# scheduled: its import finds no publication for the index, so it binds to an
# unbacked publication point. Only then is the maintenance dataflow submitted and
# scheduled, which adopts that point and wakes the import. The result peek then
# returns the correct count.
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

# The maintenance index over shard `r`, registered but not submitted, so
# `import index=2001` below can reference it while nothing is published yet.
create-dataflow name=maint-index as-of=0 defer
  import source=1000 shard=r upper=1
  build id=2000
    Project (#0, #1)
      Get u1000
  export kind=index index=2001 on=2000 key=[0]
----
deferred

# A one-column schema for the count reduce's output (a single bigint).
define-schema name=count_out
  count bigint
----
ok

# The interactive query dataflow: `count(*)` over the still unpublished
# maintenance index, exported under a transient id and bounded one step past
# `as_of`, so it is routed to the interactive runtime.
create-dataflow name=interactive-count as-of=0 until=1
  import index=2001
  build id=3000
    Reduce aggregates=[count(*)]
      Get u2000
  export index=t4000 on=3000 key=[0]
----
ok

# Scheduled before the maintenance dataflow exists, so the import is an unbacked
# point when the dataflow starts.
schedule id=t4000
----
ok

# Submitting the maintenance dataflow renders it and adopts the point. Scheduling
# it releases the publisher, which fills the point and wakes the import.
submit-dataflow name=maint-index
----
ok

schedule id=2001
----
ok

peek id=t4000 schema=count_out ts=0
----
3
