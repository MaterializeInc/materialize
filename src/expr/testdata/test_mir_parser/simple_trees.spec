# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# test being able to add sources of different types to the catalog

roundtrip
Constant <empty>
----
roundtrip OK


roundtrip
Constant type=(int64, int64)
  - (1, 2)
----
roundtrip produced a different output:
parse error at 1:9:
non-empty constants not supported yet
