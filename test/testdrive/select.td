# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> CREATE VIEW data (a, b) AS VALUES (1, 1), (2, 1), (3, 1), (1, 2)

# Don't parse 'AS OF' as a table alias.
! SELECT * FROM data AS OF;
Expected a timestamp value after 'AS OF', found: EOF

> SELECT * FROM data
1 1
2 1
3 1
1 2

> SELECT * FROM data AS OF now()
1 1
2 1
3 1
1 2
