# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_repeat_row  = true

> CREATE TABLE base (data bigint, unsigned_data uint4, diff bigint)
> CREATE MATERIALIZED VIEW data AS SELECT data, unsigned_data FROM base, repeat_row(diff)

> INSERT INTO base VALUES (1, 1, 1)
> SELECT * FROM data
1 1

> INSERT INTO base VALUES (1, 1, -1), (1, 1, -1)

> SELECT count(*) FROM data
-1

! SELECT * FROM data
contains:Invalid data in source, saw retractions (1) for row that does not exist: [Int64

> INSERT INTO base VALUES (1, 1, -1)

> SELECT count(*) FROM data
-2

! SELECT * FROM data
contains:Invalid data in source, saw retractions (2) for row that does not exist: [Int64

# regression scenario per database-issues#5246, with non-monotonic rendering
> CREATE VIEW topk AS
  SELECT grp.id, count(t.data) AS top_2_count,
           (SELECT COUNT(d.data) FROM data d WHERE d.data % 2 = grp.id) AS total_count
    FROM (SELECT generate_series(0,1) id) grp,
           LATERAL (SELECT data FROM data WHERE data % 2 = grp.id ORDER BY data LIMIT 2) t
    GROUP BY grp.id;

> CREATE DEFAULT INDEX ON topk;

! SELECT * from topk;
contains:Negative multiplicities in TopK

# regression scenario per database-issues#5224
! SELECT DISTINCT data FROM data;
contains:Non-positive multiplicity in DistinctBy

# regression scenario per database-issues#5359
! SELECT list_agg(DISTINCT data)[1] FROM data;
contains:Non-positive accumulation in ReduceInaccumulable DISTINCT

# regression scenario per database-issues#5360
! SELECT list_agg(data)[1] FROM data;
contains:Non-positive accumulation in ReduceInaccumulable

# regression scenario per database-issues#5086, with non-monotonic rendering
> CREATE VIEW max_data AS
  SELECT MAX(data) FROM data;

> CREATE DEFAULT INDEX ON max_data;

! SELECT * FROM max_data;
contains:Invalid data in source, saw non-positive accumulation

# verify that some reduction in a collation plan catches a negative
# multiplicity.
> CREATE VIEW collation AS
  SELECT
      data,
      COUNT(DISTINCT data),
      STRING_AGG(data::text || '1',  ',') AS data_1,
      MIN(data),
      MAX(DISTINCT data),
      SUM(data),
      STRING_AGG(data::text || '2',  ',') AS data_2
    FROM data
    GROUP BY data;

> CREATE DEFAULT INDEX ON collation;

! SELECT * FROM collation;
# Case-insensitive match for both 'Non' and 'non'
contains:on-positive accumulation

# Window aggregations
# These are currently commented out, because window functions currently don't check their inputs for negative
# multiplicities, see https://github.com/MaterializeInc/database-issues/issues/8568

#! SELECT list_agg(data) OVER ()[1] FROM data;
#contains:Non-positive accumulation in ReduceInaccumulable

#! SELECT sum(data) OVER () FROM data;
#contains:Non-positive accumulation in ReduceInaccumulable

#! SELECT max(data) OVER () FROM data;
#contains:Non-positive accumulation in ReduceInaccumulable

#! SELECT list_agg(unsigned_data) OVER ()[1] FROM data;
#contains:Non-positive accumulation in ReduceInaccumulable

#! SELECT sum(unsigned_data) OVER () FROM data;
#contains:Non-positive accumulation in ReduceInaccumulable

#! SELECT max(unsigned_data) OVER () FROM data;
#contains:Non-positive accumulation in ReduceInaccumulable
