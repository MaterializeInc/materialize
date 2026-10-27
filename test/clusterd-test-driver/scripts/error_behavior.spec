# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# error-behavior scenario: each command below is expected to fail; its `----`
# block holds the error message, so the failure is the assertion (no wrapper).
# Regenerate the messages with `REWRITE=1`.
#
# Input validation, caught before reaching clusterd.
# Reference to a schema that was never declared.
create-instance
----
ok

update-configuration
----
ok

initialization-complete
----
ok

write-single-ts shard=e schema=nope ts=0 count=1
----
error: unknown schema "nope"; declare it with define_schema first

# Wrong row arity: the sample schema has two columns, the row has one.
write-rows shard=e ts=0
  1
----
error: row 0 has 1 values but schema has 2 columns

# Wrong value type: text supplied for the bigint column.
write-rows shard=e ts=0
  not-an-int x
----
error: parsing "not-an-int" as int64: invalid input syntax for type bigint: invalid digit found in string: "not-an-int"

# Index key column out of range: the sample schema has two columns (0, 1).
define-index source=2000 index=2001 shard=e key=[5] as-of=0 upper=1
----
error: key column 5 out of range for a 2-column schema

# Replica behavior: an unscheduled dataflow makes no progress, so awaiting its
# output frontier times out. A strict await renders the timeout as an error.
write-single-ts shard=e ts=0 count=100
----
wrote 100

define-index source=1000 index=1001 shard=e key=[0] as-of=0 upper=1
----
ok

await-frontier id=1001 ts=1 timeout-secs=3
----
error: frontier for u1001 did not reach 1 in time
