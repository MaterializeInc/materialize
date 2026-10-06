# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# two_runtime_index scenario: a fast-path index peek on the interactive runtime.
#
# Runs against a clusterd with a second, interactive compute runtime. A
# multiplexer fronts both runtimes on the one endpoint this driver connects to,
# so the commands are those of a single-runtime script. The maintenance runtime
# renders and publishes the index, and the multiplexer routes every peek to the
# interactive runtime, so a correct result proves the interactive runtime served
# the rows from the shared registry.
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
