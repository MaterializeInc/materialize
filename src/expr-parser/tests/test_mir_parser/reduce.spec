# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Reduce without key
roundtrip
Reduce aggregates=[max(#1), min(#1), count(*)] monotonic
  Constant // { types: "(text, bigint)" }
    - ("a", 2)
    - ("a", 4)
----
roundtrip OK

# Reduce with key
roundtrip
Reduce group_by=[#0] aggregates=[max(#1), min(#1), sum(distinct #1)] monotonic exp_group_size=4
  Constant // { types: "(text, bigint)" }
    - ("a", 2)
    - ("a", 4)
----
roundtrip OK

# Distinct without key
roundtrip
Distinct monotonic
  Constant // { types: "(text, bigint)" }
    - ("a", 2)
    - ("a", 4)
----
roundtrip OK

# Distinct with key
roundtrip
Distinct group_by=[#0, #1] exp_group_size=4
  Constant // { types: "(text, bigint)" }
    - ("a", 2)
    - ("a", 4)
----
roundtrip OK
