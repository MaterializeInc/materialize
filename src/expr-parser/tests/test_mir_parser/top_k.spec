# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# TopK
roundtrip
TopK order_by=[#1 asc nulls_last, #0 desc nulls_first] limit=5
  Constant // { types: "(bigint, bigint)" }
    - (1, 2)
    - (3, 4)
----
roundtrip OK

# TopK
roundtrip
TopK group_by=[#1] limit=5 offset=2 monotonic
  Constant // { types: "(bigint, bigint)" }
    - (1, 2)
    - (3, 4)
----
roundtrip OK

# TopK
roundtrip
TopK group_by=[#1] limit=5 offset=2 monotonic exp_group_size=4
  Constant // { types: "(bigint, bigint)" }
    - (1, 2)
    - (3, 4)
----
roundtrip OK
