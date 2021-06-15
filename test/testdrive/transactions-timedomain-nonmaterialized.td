# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test behavior of timedomains with non-materialized sources. Currently they prevent transactions

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

# A SELECT from the materialized source should succeed outside a transaction.
> SELECT c FROM indexed ORDER BY c
1
2
4

! SELECT c FROM unindexed ORDER BY c
Unable to automatically determine a timestamp for your query; this can happen if your query depends on non-materialized sources.

> BEGIN

# A SELECT from the materialized source will fail in a transaction
# because the non-materialized source is in the same time domain.
# TODO(mjibson): Fix this. #6651.
! SELECT c FROM indexed ORDER BY c
Unable to automatically determine a timestamp for your query; this can happen if your query depends on non-materialized sources.

> ROLLBACK
