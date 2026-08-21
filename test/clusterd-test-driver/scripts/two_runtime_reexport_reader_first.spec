# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# two_runtime_reexport_reader_first scenario: a query dataflow on the
# interactive runtime binds to a re-exporting index before that index renders.
#
# The reader creates the alias id's publication point first, unbacked. When the
# re-export renders on the maintenance runtime it cannot alias the original's
# point, since the reader already holds a different one, so it re-imports the
# shared traces and publishes them under the alias id, backing the reader's
# point in place. The result peek then returns the correct count.
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

create-dataflow name=maint-index as-of=0
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

# Registered but not submitted, so `import index=2002` below can reference it
# while nothing is published under that id yet.
create-dataflow name=reexport as-of=0 defer
  import index=2001
  export kind=index index=2002 on=2000 key=[0]
----
deferred

define-schema name=count_out
  count bigint
----
ok

create-dataflow name=interactive-count as-of=0 until=1
  import index=2002
  build id=3000
    Reduce aggregates=[count(*)]
      Get u2000
  export index=t4000 on=3000 key=[0]
----
ok

# Scheduled before the re-export exists, so its import holds the alias id's
# unbacked point.
schedule id=t4000
----
ok

submit-dataflow name=reexport
----
ok

schedule id=2002
----
ok

peek id=t4000 schema=count_out ts=0
----
3
