# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> INSERT INTO view_table VALUES (2), (3), (3);

> CREATE MATERIALIZED VIEW view_view2 AS SELECT f1, COUNT(*) FROM view_table GROUP BY f1;

> INSERT INTO view_table VALUES (3), (4), (4);

> SELECT * FROM view_view1;
1 3
2 3
3 3
4 2
<null> 1

> SELECT * FROM view_view2;
1 3
2 3
3 3
4 2
<null> 1
