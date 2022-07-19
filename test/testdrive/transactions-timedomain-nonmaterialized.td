# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test behavior of timedomains with non-materialized sources.

$ kafka-create-topic topic=static
$ kafka-ingest topic=static format=bytes
1
2
4

> CREATE SOURCE indexed (c)
  FROM KAFKA BROKER '${testdrive.kafka-addr}'
  TOPIC 'testdrive-static-${testdrive.seed}'
  FORMAT TEXT

> CREATE DEFAULT INDEX ON indexed

> CREATE SOURCE unindexed (c)
  FROM KAFKA BROKER '${testdrive.kafka-addr}'
  TOPIC 'testdrive-static-${testdrive.seed}'
  FORMAT TEXT

> CREATE VIEW v_unindexed AS SELECT count(*) FROM unindexed

# A SELECT from the materialized source should succeed outside a transaction.
> SELECT c FROM indexed ORDER BY c
1
2
4

> SELECT c FROM unindexed
1
2
4

> SELECT * FROM v_unindexed
3

> BEGIN

# A SELECT from the materialized source in a transaction should succeed
# even though a non-materialized source is in the same time domain.
> SELECT c FROM indexed ORDER BY c
1
2
4

> SELECT c FROM unindexed
1
2
4

> COMMIT

# The unindexed view should be the same.
> BEGIN

> SELECT c FROM indexed ORDER BY c
1
2
4

> SELECT * FROM v_unindexed
3

> COMMIT

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

# Regression for #8657
# Ensure that views referencing other schemas are transitively included. Here,
# pg_catalog is generally a view over mz_catalog.
> BEGIN
> SELECT c.oid FROM pg_catalog.pg_class c LIMIT 0;
> SELECT pg_catalog.format_type(a.atttypid, a.atttypmod) FROM pg_catalog.pg_attribute a LIMIT 0;
> COMMIT

# Regression for #8942
# Ensure that non-materialized, transitive views are not included. Here,
# unindexed should not be included in the timedomain.
> CREATE MATERIALIZED VIEW v_materialized AS SELECT count(*) FROM unindexed
# Wait for the view to be updated, since we can't retry in the transaction if
# it returns a 0 timestamp error.
> SELECT * FROM v_materialized
3
> BEGIN
> SELECT * FROM v_materialized
3
> COMMIT
