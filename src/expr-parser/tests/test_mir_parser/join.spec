# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# CrossJoin
roundtrip
CrossJoin
  Constant // { types: "(bigint, bigint)" }
    - (1, 2)
    - (3, 4)
  Constant // { types: "(bigint, bigint)" }
    - (5, 6)
----
roundtrip OK

# Equi-Join
roundtrip
Join on=((#1 + #2) = #4 AND eq(#0, #3, #6) AND #5 = #8)
  Constant // { types: "(bigint, bigint, text)" }
    - (1, 2, "oh, my!")
  Constant // { types: "(bigint, bigint, text)" }
    - (3, 4, "oh, my!")
  Constant // { types: "(bigint, bigint, text)" }
    - (5, 6, "oh, my!")
----
roundtrip OK

# Equi-Join
roundtrip
Join on=((#0 + #1) = #3 AND eq(#2, #5, #8) AND #4 = (#6 + #7))
  Constant // { types: "(bigint, bigint, text)" }
    - (1, 2, "oh, my!")
  Constant // { types: "(bigint, bigint, text)" }
    - (3, 4, "oh, my!")
  Constant // { types: "(bigint, bigint, text)" }
    - (5, 6, "oh, my!")
----
roundtrip OK
