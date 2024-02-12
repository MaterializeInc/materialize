# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

roundtrip
Map ((#0 AND (#1) IS NOT NULL), #2)
  Constant // { types: "(boolean, boolean)" }
    - (false, true)
----
roundtrip OK

# Two CTEs (output differs due to pretty-printing limitations of boolean expressions)
roundtrip
Map (#0 AND #1 AND #5 OR #2 AND #3 OR #4 = #5 AND #6 IS NULL)
  Constant // { types: "(boolean, boolean, boolean, boolean, boolean, boolean, boolean)" }
    - (true, false, true, false, false, true, false)
----
----
roundtrip produced a different output:
~~~ expected:
Map (#0 AND #1 AND #5 OR #2 AND #3 OR #4 = #5 AND #6 IS NULL)
  Constant
    - (true, false, true, false, false, true, false)

~~~ actual:
Map (((#0 AND #1 AND #5) OR (#2 AND #3) OR ((#4 = #5) AND (#6) IS NULL)))
  Constant
    - (true, false, true, false, false, true, false)

~~~
----
----
