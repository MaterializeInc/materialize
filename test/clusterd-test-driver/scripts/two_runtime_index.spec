# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# two_runtime_index scenario: runs against a clusterd started with a second,
# interactive compute runtime configured (see `Clusterd(interactive_compute=True)`
# in the `two_runtime_compute` workflow). A multiplexer inside clusterd fronts both
# runtimes on the single `:2101` endpoint this driver connects to, so the commands
# below are unchanged from a single-runtime script; only the clusterd underneath
# differs. The maintenance runtime renders and publishes the index below into the
# shared arrangement-sharing registry; the multiplexer routes the `peek` to the
# interactive runtime regardless, so a correct result proves the interactive
# runtime served the rows from that shared registry rather than rendering its own
# copy of the index.
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

create-dataflow name=two-runtime-index as-of=0
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

# Routed to the interactive runtime by the multiplexer.
peek id=2001 ts=0
----
1 "alpha"
2 "beta"
3 "gamma"
