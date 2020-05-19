# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> CREATE VIEW v AS VALUES (1, 'foo'), (2, 'bar'), (3, 'foo'), (1, 'bar')

> SELECT * FROM v;
1 "foo"
1 "bar"
2 "bar"
3 "foo"

> CREATE TEMPORARY VIEW temp_v AS SELECT * FROM v;

> SELECT * FROM temp_v;
1 "foo"
1 "bar"
2 "bar"
3 "foo"

> SELECT * FROM mz_temp.temp_v;
1 "foo"
1 "bar"
2 "bar"
3 "foo"

! CREATE VIEW non_temp AS SELECT * FROM temp_v;
non-temporary items cannot depend on temporary item

> CREATE TEMP VIEW double_temp_v AS SELECT * FROM temp_v;

> SELECT * FROM double_temp_v;
1 "foo"
1 "bar"
2 "bar"
3 "foo"

> SELECT * FROM mz_temp.double_temp_v;
1 "foo"
1 "bar"
2 "bar"
3 "foo"

! DROP VIEW temp_v;
cannot drop mz_temp.temp_v: still depended upon by catalog item 'mz_temp.double_temp_v'

> DROP VIEW double_temp_v;

> DROP VIEW temp_v;

! SELECT * FROM double_temp_v;
unknown catalog item 'double_temp_v'

! SELECT * FROM temp_v;
unknown catalog item 'temp_v'
