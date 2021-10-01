# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test behavior of timedomains with non-materialized sources.

$ file-append path=static.csv
1
2
4

> CREATE MATERIALIZED SOURCE indexed (c)
  FROM FILE '${testdrive.temp-dir}/static.csv'
  FORMAT CSV WITH 1 COLUMNS

> CREATE SOURCE unindexed (c)
  FROM FILE '${testdrive.temp-dir}/static.csv'
  FORMAT CSV WITH 1 COLUMNS

> CREATE VIEW v_unindexed AS SELECT count(*) FROM unindexed

# A SELECT from the materialized source should succeed outside a transaction.
> SELECT c FROM indexed ORDER BY c
1
2
4

! SELECT * FROM unindexed
unable to automatically determine a query timestamp

! SELECT * FROM v_unindexed
unable to automatically determine a query timestamp

> BEGIN

# A SELECT from the materialized source in a transaction should succeed
# even though a non-materialized source is in the same time domain.
> SELECT c FROM indexed ORDER BY c
1
2
4

! SELECT c FROM unindexed ORDER BY c
Transactions can only reference objects in the same timedomain

> ROLLBACK

# The unindexed view should be the same.
> BEGIN

> SELECT c FROM indexed ORDER BY c
1
2
4

! SELECT * FROM v_unindexed
Transactions can only reference objects in the same timedomain

> ROLLBACK

# Ensure that other optionally indexed things (views) are correctly
# included in the timedomain.
> CREATE VIEW v AS SELECT COUNT(*) FROM indexed

# Wait until there are results.
> SELECT * FROM v
3

> BEGIN

> SELECT c FROM indexed ORDER BY c
1
2
4

> SELECT * FROM v
3

> COMMIT

# Make v indexed to ensure it works too.
> CREATE DEFAULT INDEX ON v;

# Wait until there are results.
> SELECT * FROM v
3

> BEGIN

> SELECT c FROM indexed ORDER BY c
1
2
4

> SELECT * FROM v
3

> COMMIT
