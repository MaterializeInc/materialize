# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# two_runtime_reexport_interactive scenario: a query dataflow on the interactive
# runtime whose export is the arrangement it imports.
#
# The dataflow imports a maintenance index and exports it under a transient id
# with the same key, so the export finds the imported shared arrangement rather
# than a freshly rendered one. On the interactive runtime that export becomes an
# alias of the index's publication point, with no arrangement of its own. The
# peek names only the transient id, so a correct result proves the alias reaches
# the shared arrangement.
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

# Same collection, same key, exported under a transient id and bounded one step
# past `as_of`, so the multiplexer routes it to the interactive runtime.
create-dataflow name=interactive-reexport as-of=0 until=1
  import index=2001
  export kind=index index=t3000 on=2000 key=[0]
----
ok

schedule id=t3000
----
ok

peek id=t3000 ts=0
----
1 "alpha"
2 "beta"
3 "gamma"
