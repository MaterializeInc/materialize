# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# An interpret test case specifies, in order:
# - The column types.
# - Literal values for each column. Our spec for that column will be the union of the
#   specs of the column's values.
# - The expression. We'll interpret the expression to get an output spec.
# - Literal values to test the output spec against.

# Is this thing on? Check that basic integer ranges work as expected.
interpret
types (integer?)
values (4, 6)
expr #0
test (3, 5, 7, "test")
----
may contain: [5]

# A temporal-style filter: compare a value to some function of the column.
# Expression: (2100 >= 300 + insert_ms)
interpret
types (text?, numeric?)
values ("hello", "goodbye")
values (2E+3::numeric, 2.05E+3::numeric)
expr (2.1E+3::numeric >= add_numeric(3E+2::numeric, #1))
test (true, false, null::boolean)
----
may contain: [false <err>]

# The same, but the filter matches.
# Expression: (2900 >= 300 + insert_ms)
interpret
types (text?, numeric?)
values ("hello", "goodbye")
values (2E+3::numeric, 2.05E+3::numeric)
expr (2.9E+3::numeric >= add_numeric(3E+2::numeric, #1))
test (true, false, null::boolean)
----
may contain: [true <err>]

# JSONB ->
# Expression: (json_col -> "created_ms")
interpret
types (jsonb?)
values ("{\"created_ms\":1000}"::jsonb, "{\"created_ms\":2000}"::jsonb, "{\"created_ms\":3000}"::jsonb)
expr jsonb_get_string(#0, "created_ms")
test (0::numeric, 2.05E+3::numeric, 4E+3::numeric, null::boolean)
----
may contain: [2050]

# JSONB ->> (string column)
# Expression: (json_col ->> "code")
interpret
types (jsonb?)
values ("{\"code\":\"00135\"}"::jsonb, "{\"code\":\"22122\"}"::jsonb, "{\"code\":\"34153\"}"::jsonb)
expr jsonb_get_string_stringify(#0, "code")
test ("00000", "20000", "2", "80000", null::boolean)
----
may contain: ["20000" "2"]

# JSONB ->> (numeric column... unsupported)
# Expression: (json_col ->> "created_ms")
interpret
types (jsonb?)
values ("{\"created_ms\":1000}"::jsonb, "{\"created_ms\":2000}"::jsonb, "{\"created_ms\":3000}"::jsonb)
expr jsonb_get_string_stringify(#0, "created_ms")
test ("00000", "20000", "2", "80000", null::boolean)
----
may contain: ["00000" "20000" "2" "80000"]

# JSONB -> (nulls)
# Expression: (json_col -> "created_ms")
interpret
types (jsonb?)
values (null::boolean, null::boolean, null::boolean)
expr jsonb_get_string(#0, "created_ms")
test ("00000", "foo", null::boolean, true)
----
may contain: [null]

# Regression test: `or` may short circuit even when the first argument throws an error
# Expression: ((((1 / 0) > 0) OR true))
interpret
types ()
expr ((div_numeric(1::numeric, 0::numeric) > 0::numeric) OR true)
test ("string", 300, true, false, null::boolean)
----
may contain: [true <err>]

# Functions with many arguments can be expensive to interpret. 5 arguments is below the limit; note that
# the output spec contains the exact value.
#
# Expression: jsonb_array_length(jsonb_build_array(true, true))
interpret
types ()
expr jsonb_array_length(jsonb_build_array(true, true, true, true, true))
test ("string", 5::integer, 6::integer, true, false, null::boolean)
----
may contain: [5]

# Here, the interpreter has given up on interpreting a function with 6+ arguments; any value is possible.
# Expression: jsonb_array_length(jsonb_build_array(true, true, true, true, true, true))
interpret
types ()
expr jsonb_array_length(jsonb_build_array(true, true, true, true, true, true))
test ("string", 5::integer, 6::integer, true, false, null::boolean)
----
may contain: ["string" 5 6 true false null <err>]

# And for associative functions like COALESCE, we can handle even long argument lists.
# Expression: coalesce(true, true, true, true, true, true, true, true)
interpret
types ()
expr coalesce(true, true, true, true, true, true, true, true)
test ("string", 7::integer, 8::integer, true, false, null::boolean)
----
may contain: [true]

# Expression: #0 = #1
interpret
types (numeric?, numeric?)
values (0::numeric, 1::numeric)
values (1::numeric, 2::numeric)
expr (#0 = #1)
test (true, false, null::boolean, 13)
----
may contain: [true false]

interpret
types (numeric?, numeric?)
values (1::numeric)
values (1::numeric)
expr (#0 = #1)
test (true, false, null::boolean, 13)
----
may contain: [true]

interpret
types (numeric?, numeric?)
values (0::numeric)
values (2::numeric)
expr (#0 = #1)
test (true, false, null::boolean, 13)
----
may contain: [false]

interpret
types (numeric?, numeric?)
values (0::numeric, null::boolean)
values (2::numeric, null::boolean)
expr (#0 = #1)
test (true, false, null::boolean, 13)
----
may contain: [false null]
