# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# two_runtime_reexport scenario: a peek on an index that re-exports another
# index's arrangement.
#
# The second index's dataflow imports the first and exports the imported
# arrangement under its own id, so on the maintenance runtime it renders no
# operators of its own and registers its id as an alias of the first index's
# publication point. The peeks and the query dataflow below name only the
# alias, so a correct result proves the interactive runtime reached the shared
# arrangement through it.
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

# Same collection, same key: the export finds the imported arrangement and
# re-exports it rather than arranging again.
create-dataflow name=reexport as-of=0
  import index=2001
  export kind=index index=2002 on=2000 key=[0]
----
ok

schedule id=2002
----
ok

# A fast-path peek through the alias.
peek id=2002 ts=0
----
1 "alpha"
2 "beta"
3 "gamma"

define-schema name=count_out
  count bigint
----
ok

# A query dataflow importing the alias, routed to the interactive runtime by
# `until = as_of + 1`.
create-dataflow name=interactive-count as-of=0 until=1
  import index=2002
  build id=3000
    Reduce aggregates=[count(*)]
      Get u2000
  export index=t4000 on=3000 key=[0]
----
ok

schedule id=t4000
----
ok

peek id=t4000 schema=count_out ts=0
----
3
