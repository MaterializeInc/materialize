# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

define
DefSource name=x
  - c0: bigint?
  - c1: bigint?
  - c2: bigint?
----
Source defined as t0

define
DefSource name=t1
  - c0: integer?
  - c1: integer?
----
Source defined as t1

define
DefSource name=t2
  - c0: integer?
  - c1: integer?
----
Source defined as t2

# regression test for materialize#8235
apply pipeline=CanonicalizeMfp
Project (#1)
  Filter (add_int64(null::boolean, #3) = 5::integer)
    Map (add_int64(#1, #2))
      Get x
----
Project (#1)
  Filter false
    Get x

# pushdown filters with a map. filters should be fused and re-sorted with the expression inlined
apply pipeline=CanonicalizeMfp
Project (#3)
  Filter (#3 > 1234::integer)
    Filter (#3 < 4321::integer)
      Map (add_int64(#0, #1))
        Get x
----
Project (#3)
  Filter (#3 < 4321) AND (#3 > 1234)
    Map ((#0 + #1))
      Get x

# multiple pushdown filters without a map. filters should be fused and re-sorted least to greatest
apply pipeline=CanonicalizeMfp
Project (#0, #1)
  Filter (#1 > 4321::integer)
    Filter (#0 < 1234::integer)
      Get x
----
Project (#0, #1)
  Filter (#0 < 1234) AND (#1 > 4321)
    Get x

# multiple retained filters with a map. canonicalized filters should be re-sorted least to greatest
apply pipeline=CanonicalizeMfp
Project (#0, #3, #4)
  Filter ((#4) IS NULL) AND ((#3) IS NULL)
    Map (hmac_string(#0, #1, #2), hmac_string(#0, #1))
      Get x
----
Project (#0, #3, #4)
  Filter (#3) IS NULL AND (#4) IS NULL
    Map (hmac(#0, #1, #2), hmac(#0, #1))
      Get x

apply pipeline=CanonicalizeMfp
Project (#0, #3)
  Filter (#0 < 1234) AND ((#3) IS NULL)
    Map (hmac_string(#0, #1, #2))
      Get x
----
Project (#0, #3)
  Filter (#3) IS NULL AND (#0 < 1234)
    Map (hmac(#0, #1, #2))
      Get x

# regression test for materialize#10000.
# Even though there is no map in the test, the duplicated predicates cause the creation
# of a map via memoizing common subexpressions that then gets optimized away.
apply pipeline=CanonicalizeMfp
Project (#0)
  Filter not(((#0) IS NULL)) AND ((#0 = 5::integer) OR (#0 = 1337::integer)) AND ((#0 = 5::integer) OR (#0 = 1337::integer))
    Project (#0)
      Join on=(#0 = #2)
        Get t1
        Get t2
----
Project (#0)
  Filter (#0) IS NOT NULL AND ((#0 = 5) OR (#0 = 1337))
    Join on=(#0 = #2)
      Get t1
      Get t2

# same test as above, but with predicates that are equivalent only after considering the innermost map-project
apply pipeline=CanonicalizeMfp
Project (#0)
  Filter not(((#0) IS NULL)) AND ((add_int64(#0, #2) = 5::integer) OR (add_int64(#0, #1) = 9::integer)) AND ((#3 = 5::integer) OR (#3 = 9::integer))
    Map (add_int64(#0, #2))
      Project (#0, #1, #1)
        Join on=(#0 = #2)
          Get t1
          Get t2
----
Project (#0)
  Filter (#0) IS NOT NULL AND ((#4 = 5) OR (#4 = 9))
    Map ((#0 + #1))
      Join on=(#0 = #2)
        Get t1
        Get t2

# consecutive levels of map-filter-project. outermost mfp is the same as the materialize#10000 regression test.
apply pipeline=CanonicalizeMfp
Project (#0, #3)
  Filter not(((#0) IS NULL)) AND ((#0 = 5::integer) OR (#0 = 1337::integer)) AND ((#0 = 5::integer) OR (#0 = 1337::integer))
    Project (#0, #1, #2, #3)
      Join on=(#0 = #2)
        Project (#2)
          Filter (#2 > 1234)
            Map (add_int64(#0, #1))
              Get t1
        Map (mul_int64(#0, #1))
          Get t2
----
Project (#0, #3)
  Filter ((#0 = 5) OR (#0 = 1337))
    Join on=(#0 = #2)
      Project (#2)
        Filter (#2 > 1234)
          Map ((#0 + #1))
            Get t1
      Map ((#0 * #1))
        Get t2

apply pipeline=CanonicalizeMfp
Project (#2)
  Map (add_int64(#0, #1))
    Filter not(((#0) IS NULL)) AND ((#0 = 5::integer) OR (#0 = 1337::integer)) AND ((#0 = 5::integer) OR (#0 = 1337::integer))
      Project (#0, #2)
        Join on=(#0 = #2)
          Get t1
          Get t2
----
Project (#4)
  Filter (#0) IS NOT NULL AND ((#0 = 5) OR (#0 = 1337))
    Map ((#0 + #2))
      Join on=(#0 = #2)
        Get t1
        Get t2
