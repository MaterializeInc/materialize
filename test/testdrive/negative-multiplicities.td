# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> CREATE TABLE base (data bigint, diff bigint)
> CREATE MATERIALIZED VIEW data AS SELECT data FROM base, repeat_row(diff)

> INSERT INTO base VALUES (1, 1)
> SELECT * FROM data
1

> INSERT INTO base VALUES (1, -1), (1, -1)

> SELECT count(*) FROM data
-1

! SELECT * FROM data
contains:Invalid data in source, saw retractions (1) for row that does not exist: [Int64(1)]

> INSERT INTO base VALUES (1, -1)

> SELECT count(*) FROM data
-2

! SELECT * FROM data
contains:Invalid data in source, saw retractions (2) for row that does not exist: [Int64(1)]

# regression scenario per #17963
! SELECT grp.id, count(t.data) AS top_2_count,
         (SELECT COUNT(d.data) FROM data d WHERE d.data % 2 = grp.id) AS total_count
  FROM (SELECT generate_series(0,1) id) grp,
         LATERAL (SELECT data FROM data WHERE data % 2 = grp.id ORDER BY data LIMIT 2) t
  GROUP BY grp.id;
contains:Negative multiplicities in TopK
