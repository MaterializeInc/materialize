# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Test that check that various overflow and carry-over conditions are handled
#

# Value becomes greater than 1

> SELECT '0.999999999999999999999999999999999999999'::decimal + '0.000000000000000000000000000000000000001'::decimal
1

# Division creates a number that is too small
! SELECT '0.000000000000000000000000000000000000001'::decimal / 10::decimal;
contains:value out of range: underflow

# Division creates a number that is too large
! SELECT '999999999999999999999999999999999999999'::decimal / 0.1::decimal;
contains:value out of range: overflow

# Multilication creates a number that is too small
! SELECT '0.000000000000000000000000000000000000001'::decimal * 0.1::decimal;
contains:value out of range: underflow

# Multiplication creates a number that is too large
! SELECT '999999999999999999999999999999999999999'::decimal * 10::decimal;
contains:value out of range: overflow

# ROUND creates a value that is too large
! SELECT ROUND('999999999999999999999999999999999999999'::decimal,1);
contains:value out of range: overflow

# POW
! SELECT POW(99999::decimal,9);
contains:value out of range: overflow

# Conversion from double
! SELECT 999999999999999999999999999999999999999::double::decimal;
contains:numeric field overflow
