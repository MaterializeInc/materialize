# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test SQL functions.

> SELECT split_part('abc~@~def~@~ghi', '~@~', 2);
split_part
---------
def

> SELECT split_part('abc~@~def~@~ghi', '', 1);
split_part
---------
abc~@~def~@~ghi

> SELECT split_part('abc~@~def~@~ghi', '~@~', 4);
split_part
----------
""

> SELECT split_part('hello there', '', 1);
split_part
---------
"hello there"

! SELECT split_part('abc~@~def~@~ghi', '~@~', 0);
integer out of range

! SELECT split_part();
Cannot call function split_part(): arguments cannot be implicitly cast to any implementation's parameters; try providing explicit casts

! SELECT split_part('one', 'two');
Cannot call function split_part(unknown, unknown): arguments cannot be implicitly cast to any implementation's parameters; try providing explicit casts

! SELECT split_part(1, 2, 3);
Cannot call function split_part(i32, i32, i32): arguments cannot be implicitly cast to any implementation's parameters; try providing explicit casts
