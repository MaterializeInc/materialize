# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# The syntax for an mfp test is `<input_arity> [<commands>]`
# The syntax for a command is `(<name_of_command> [<args>])`

mfp
arity 1
map (add_int64(#0, 1), add_int64(add_int64(#0, 1), 1))
filter ((#2 = 3))
project (0, 1, 2)
optimize
----
[(#0 + 1) (#1 + 1)]
[(#2 = 3)]
[0 1 2]

mfp
arity 1
map (add_int64(#0, 1), add_int64(add_int64(#0, 1), 1))
filter ((#2 = 3))
project (2)
optimize
----
[((#0 + 1) + 1)]
[(#1 = 3)]
[1]

mfp
arity 1
map (add_int64(#0, 1), add_int64(add_int64(#0, 1), 1))
filter ((#1 = 3))
project (2)
optimize
----
[(#0 + 1) (#1 + 1)]
[(#1 = 3)]
[2]

# duplicate filters

mfp
arity 1
map (((#0) IS NULL), (#0 > 1))
filter (#1, #2, #1)
project (0)
optimize
----
[]
[(#0) IS NULL (#0 > 1)]
[0]
