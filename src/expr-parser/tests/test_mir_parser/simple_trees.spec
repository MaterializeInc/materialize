# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Source definitions
# ------------------

# Define t0 source
define
DefSource name=t0 keys=[[#0]]
  - bigint
  - bigint
----
Source defined as t0

# Define t1 source
define
DefSource name=t1 keys=[[#0]]
  - text
  - bigint
  - boolean
----
Source defined as t1

# Constant empty
roundtrip
Constant <empty> // { types: "()" }
----
roundtrip OK

# Constant with values
roundtrip
Constant // { types: "(bigint, double precision, text)" }
  - ((1, 2.5, "bar") x -1)
  - (15, 4.2, "baz")
  - ((1, 2.73, "foobar") x -1)
----
roundtrip OK

# Get global
roundtrip
Get t0
----
roundtrip OK

# Project
roundtrip
Project (#0, #2)
  Get t1
----
roundtrip OK

# Map + Scalar functions (part 1)
roundtrip
Map ((#0) IS NOT NULL, (#1) IS NOT TRUE, NOT(#1), (#0 + #0), (#0 * #0))
  Map (error("internal error: oops"))
    Constant // { types: "(bigint, boolean)" }
      - (0, true)
----
roundtrip OK

# Map + Scalar functions (part 2)
roundtrip
Map (mz_environment_id(), abs(#0), ltrim("testltrim", #1), greatest(1, 2, 3))
  Map (42, "best", #0)
    Constant // { types: "()" }
      - ()
----
roundtrip OK

# FlatMap
roundtrip
FlatMap unnest_list(#0)
  Constant // { types: "()" }
    - ()
----
roundtrip OK

# Filter
roundtrip
Filter #0 AND NOT((#1 + #1) >= #2)
  Filter error("internal error: oops")
    Constant // { types: "(boolean, bigint, bigint)" }
      - (true, 1, 2)
----
----
roundtrip produced a different output:
~~~ expected:
Filter #0 AND NOT((#1 + #1) >= #2)
  Filter error("internal error: oops")
    Constant
      - (true, 1, 2)

~~~ actual:
Filter #0 AND NOT(((#1 + #1) >= #2))
  Filter error("internal error: oops")
    Constant
      - (true, 1, 2)

~~~
----
----

# Threshold
roundtrip
Threshold
  Constant <empty> // { types: "(bigint, bigint)" }
----
roundtrip OK

# Union
roundtrip
Union
  Union
    Constant <empty> // { types: "(bigint, bigint)" }
    Constant // { types: "(bigint, bigint)" }
      - (1, 2)
  Union
    Constant // { types: "(bigint, bigint)" }
      - (3, 4)
----
roundtrip OK
