# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# This test checks that the various decimal representations of zero and numbers very close to zero behave as expected
#

> CREATE TABLE decimal_zero (f1 DECIMAL);
> INSERT INTO decimal_zero VALUES (0), (-0), ('0.0'::decimal), ('00000000000000000.0'::decimal), ('-0.000000000000000000000'::decimal);

> SELECT DISTINCT f1, f1::decimal(5,2)::text FROM decimal_zero;
0 0.00

> SELECT MIN(f1)::text, MAX(f1)::text FROM decimal_zero;
0 0

> SELECT COUNT(DISTINCT f1)::text, SUM(DISTINCT f1)::text FROM decimal_zero;
1 0

> SELECT f1::text FROM decimal_zero ORDER BY f1;
0
0
0
0
0

> SELECT COUNT(*) FROM decimal_zero AS a1, decimal_zero AS a2 WHERE a1.f1 = a2.f1;
25

! SELECT 123 / '-0'::decimal;
contains:division by zero

> SELECT '0.000000000000000000000000000000000000001'::decimal - '0.000000000000000000000000000000000000001'::decimal = 0;
true

> SELECT '-0.000000000000000000000000000000000000001'::decimal + '0.000000000000000000000000000000000000001'::decimal = 0;
true

> SELECT '0.000000000000000000000000000000000000001'::decimal + '-0.000000000000000000000000000000000000001'::decimal = 0;
true

> SELECT '1'::decimal - '1'::decimal UNION DISTINCT SELECT '1.1'::decimal - '1.10'::decimal;
0

! SELECT '0.000000000000000000000000000000000000001'::decimal * '-0.000000000000000000000000000000000000001'::decimal;
contains:value out of range: underflow

! SELECT '0.1'::decimal / 999999999999999999999999999999999999999::decimal;
contains:value out of range: underflow
