# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# ArrangeBy

roundtrip
ArrangeBy keys=[[#0], [#1]]
  Constant // { types: "(bigint, bigint)" }
    - (1, 2)
    - (3, 4)
----
roundtrip OK

# NB this doesn't typecheck, but the parser still accepts it
roundtrip
ArrangeBy keys=[[#1], [#2, #3]]
  Constant // { types: "(bigint, bigint)" }
    - (1, 2)
    - (3, 4)
----
roundtrip OK
