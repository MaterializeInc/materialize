# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# These are only errors which have no associated issues since they are not
# considered product bugs, but SQLsmith generating bad queries. Use ci-regexp
# in Github issues for actual product bugs.
known_errors = [
    "no connection to the server",  # Expected AFTER a crash, the query before this is interesting, not the ones after
    "failed: Connection refused",  # Expected AFTER a crash, the query before this is interesting, not the ones after
    "could not translate host name",  # Expected AFTER a crash, the query before this is interesting, not the ones after
    "canceling statement due to statement timeout",
    "value too long for type",
    "list_agg on char not yet supported",
    "does not allow subqueries",
    "function array_remove(",  # insufficient type system, parameter types have to match
    "function array_cat(",  # insufficient type system, parameter types have to match
    "function array_position(",  # insufficient type system, parameter types have to match
    "function list_append(",  # insufficient type system, parameter types have to match
    "function list_prepend(",  # insufficient type system, parameter types have to match
    "function list_cat(",  # insufficient type system, parameter types have to match
    "does not support implicitly casting from",
    "aggregate functions that refer exclusively to outer columns not yet supported",  # https://github.com/MaterializeInc/database-issues/issues/1163
    "range lower bound must be less than or equal to range upper bound",
    "violates not-null constraint",
    "division by zero",
    "zero raised to a negative power is undefined",
    "operator does not exist",  # For list types
    "couldn't parse role id",
    "mz_aclitem grantor cannot be PUBLIC role",
    "unrecognized privilege type:",
    "cannot return complex numbers",
    "statement batch size cannot exceed",
    "length must be nonnegative",
    "is only defined for finite arguments",
    "more than one record produced in subquery",
    "invalid range bound flags",
    "invalid input syntax for type jsonb",
    "invalid regular expression",
    "invalid input syntax for type date",
    "invalid escape string",
    "invalid hash algorithm",
    "is defined for numbers greater than or equal to",
    "is not defined for zero",
    "is not defined for negative numbers",
    "requested character too large for encoding",
    "requested character not valid for encoding",
    "internal error: unrecognized configuration parameter",
    "invalid encoding name",
    "invalid time zone",
    "value out of range: overflow",
    "value out of range: underflow",
    "LIKE pattern exceeds maximum length",
    "negative substring length not allowed",
    "cannot take square root of a negative number",
    "timestamp units not yet supported",
    "step size cannot equal zero",
    "stride must be greater than zero",
    "timestamp out of range",
    "integer out of range",
    "unterminated escape sequence in LIKE",
    "null character not permitted",
    "is defined for numbers between",
    "field position must be greater than zero",
    "array_fill on ",  # Not yet supported
    "must not be null",  # Expected with array_fill, array_position
    "' not recognized",  # Expected, see https://github.com/MaterializeInc/database-issues/issues/5253
    "must appear in the GROUP BY clause or be used in an aggregate function",
    "Expected joined table, found",  # Should fix for multi table join
    "Expected ON, or USING after JOIN, found",  # Should fix for multi table join
    "but expression is of type",  # Should fix, but only happens rarely
    "coalesce could not convert type map",  # Should fix, but only happens rarely
    "operator does not exist: map",  # Should fix, but only happens rarely
    "result exceeds max size of",  # Seems expected with huge queries
    "expected expression, but found reserved keyword",  # Should fix, but only happens rarely with subqueries
    "Expected right parenthesis, found left parenthesis",  # Should fix, but only happens rarely with cast+coalesce
    "invalid selection: operation may only refer to user-defined tables",  # Seems expected when using catalog tables
    "Unsupported temporal predicate",  # Expected, see https://github.com/MaterializeInc/database-issues/issues/5288
    "Unsupported temporal operation: NotEq",
    "Unsupported binary temporal operation: NotEq",
    "OneShot plan has temporal constraints",  # Expected, see https://github.com/MaterializeInc/database-issues/issues/5288
    "internal error: cannot evaluate unmaterializable function",  # Currently expected, see https://github.com/MaterializeInc/database-issues/issues/4083
    "string is not a valid identifier:",  # Expected in parse_ident & quote_ident
    "invalid datepart",
    "pg_cancel_backend in this position not yet supported",
    "unrecognized configuration parameter",
    "numeric field overflow",
    "bigint out of range",
    "smallint out of range",
    "uint8 out of range",
    "uint4 out of range",
    "uint2 out of range",
    "interval out of range",
    "timezone interval must not contain months or years",
    "not supported for type date",
    "not supported for type time",
    "coalesce types text and text list cannot be matched",  # Bad typing for ||
    "coalesce types text list and text cannot be matched",  # Bad typing for ||
    "is out of range for type numeric: exceeds maximum precision",
    "CAST does not support casting from ",  # TODO: Improve type system
    "SET clause does not support casting from ",  # TODO: Improve type system
    "coalesce types integer and interval cannot be matched",  # TODO: Implicit cast from timestamp to date in (date - timestamp)
    "coalesce types interval and integer cannot be matched",  # TODO: Implicit cast from timestamp to date in (date - timestamp)
    "requested length too large",
    "number of columns must be a positive integer literal",
    "regexp_extract requires a string literal as its first argument",
    "regex parse error",
    "out of valid range",
    '" does not exist',  # role does not exist
    "attempt to create relation with too many columns",
    "target replica failed or was dropped",  # expected on replica OoMs with materialize#21587
    "cannot materialize call to",  # create materialized view on some internal views
    "arrays must not contain null values",  # aclexplode, mz_aclexplode
    "OVER clause not allowed on",  # window functions
    "cannot reference pseudo type",
    "window functions are not allowed in table function arguments",  # TODO: Remove when database-issues#6317 is implemented
    "window functions are not allowed in OR argument",  # wrong error message
    "window functions are not allowed in AND argument",  # wrong error message
    "window functions are not allowed in aggregate function",
    "invalid IANA Time Zone Database identifier",
    "Top-level LIMIT must be a constant expression",
    "LIMIT must not be negative",
    "materialized view objects cannot depend on log sources",  # explain create materialized view
    "aggregate functions are not allowed in OR argument",
    "aggregate functions are not allowed in AND argument",
    "aggregate functions are not allowed in WHERE clause",
    "aggregate functions are not allowed in table function arguments",
    "aggregate functions are not allowed in LIMIT",
    "nested aggregate functions are not allowed",
    "function map_build(text list) does not exist",
    "timestamp cannot be NaN",
    "exceeded recursion limit of 2048",
    "key cannot be null",  # expected, see PR materialize#25941
    "regexp_extract must specify at least one capture group",
    "array_fill with arrays not yet supported",
    "not yet supported",
    "The fast_path_optimizer shouldn't make a fast path plan slow path.",  # TODO: Remove when database-issues#9645 is fixed
    "Window function performance issue: `reduce_unnest_list_fusion` failed",  # TODO: Remove when database-issues#9644 is fixed
]
