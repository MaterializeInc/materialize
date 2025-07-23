# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> SET transaction_isolation = 'strict serializable'

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_alter_swap = true;

> CREATE SCHEMA blue;

> CREATE SCHEMA green;

$ set-from-sql var=og-blue-schema-id
SELECT id FROM mz_schemas WHERE name = 'blue';

$ set-from-sql var=og-green-schema-id
SELECT id FROM mz_schemas WHERE name = 'green';

> BEGIN;

> ALTER SCHEMA blue SWAP WITH green;

> COMMIT;

> SELECT name FROM mz_schemas WHERE id = '${og-blue-schema-id}'
"green"

> BEGIN

> ALTER SCHEMA green RENAME TO purple;

> ALTER SCHEMA purple RENAME TO orange;

> ALTER SCHEMA orange RENAME TO green;

> ALTER SCHEMA green SWAP WITH blue;

> COMMIT

> SELECT name FROM mz_schemas WHERE ID = '${og-blue-schema-id}'
"blue"

> BEGIN

> ALTER SCHEMA blue RENAME TO purple

> ROLLBACK

# Should stay blue since we rolled back the transaction.

> SELECT name FROM mz_schemas WHERE ID = '${og-blue-schema-id}'
"blue"

# Cleanup.

> DROP SCHEMA blue
> DROP SCHEMA green

# Mock out a real-ish Blue/Green scenario.

> CREATE TABLE source_data (x int)

> INSERT INTO source_data VALUES (10), (20), (30)

> CREATE CLUSTER blue_compute SIZE '1'
> CREATE CLUSTER blue_serving SIZE '1'

> CREATE SCHEMA blue

> CREATE MATERIALIZED VIEW blue.mv1 IN CLUSTER blue_compute AS ( SELECT SUM(x) FROM source_data );
> CREATE DEFAULT INDEX IN CLUSTER blue_serving ON blue.mv1;

> SET cluster TO blue_serving;

> SELECT * FROM blue.mv1;
60

? EXPLAIN OPTIMIZED PLAN AS VERBOSE TEXT FOR SELECT * FROM blue.mv1;
Explained Query (fast path):
  ReadIndex on=materialize.blue.mv1 mv1_primary_idx=[*** full scan ***]

Used Indexes:
  - materialize.blue.mv1_primary_idx (*** full scan ***)

Target cluster: blue_serving

> SET cluster TO quickstart;

# Spin up a new stack.

> CREATE CLUSTER green_compute SIZE '1'
> CREATE CLUSTER green_serving SIZE '1'

> CREATE SCHEMA green

> CREATE MATERIALIZED VIEW green.mv1 IN CLUSTER green_compute AS ( SELECT AVG(x) FROM source_data );
> CREATE DEFAULT INDEX IN CLUSTER green_serving ON green.mv1;

> SET cluster to green_serving;

> SELECT * FROM green.mv1;
20

? EXPLAIN OPTIMIZED PLAN AS VERBOSE TEXT FOR SELECT * FROM green.mv1;
Explained Query (fast path):
  ReadIndex on=materialize.green.mv1 mv1_primary_idx=[*** full scan ***]

Used Indexes:
  - materialize.green.mv1_primary_idx (*** full scan ***)

Target cluster: green_serving

> SET cluster to defaut;

# Do the swap!

$ set-from-sql var=og-green-schema-id
SELECT id FROM mz_schemas WHERE name = 'green';

$ set-from-sql var=og-green-compute-id
SELECT id FROM mz_clusters WHERE name = 'green_compute';

$ set-from-sql var=og-green-serving-id
SELECT id FROM mz_clusters WHERE name = 'green_serving';

> BEGIN;

> ALTER SCHEMA blue SWAP WITH green;
> ALTER CLUSTER blue_serving SWAP WITH green_serving;
> ALTER CLUSTER blue_compute SWAP WITH green_compute;

> COMMIT;

# Validate the swap.

> SET CLUSTER to blue_serving;

> SELECT * FROM blue.mv1;
20

? EXPLAIN OPTIMIZED PLAN AS VERBOSE TEXT FOR SELECT * FROM blue.mv1;
Explained Query (fast path):
  ReadIndex on=materialize.blue.mv1 mv1_primary_idx=[*** full scan ***]

Used Indexes:
  - materialize.blue.mv1_primary_idx (*** full scan ***)

Target cluster: blue_serving

> SELECT name FROM mz_schemas WHERE id = '${og-green-schema-id}';
"blue"

> SELECT name FROM mz_clusters WHERE id = '${og-green-compute-id}';
"blue_compute"

> SELECT name FROM mz_clusters WHERE id = '${og-green-serving-id}';
"blue_serving"

# Drop unused resources.

> DROP CLUSTER green_compute CASCADE;
> DROP CLUSTER green_serving CASCADE;

# Make sure everything still works.

> SELECT * FROM blue.mv1;
20

? EXPLAIN OPTIMIZED PLAN AS VERBOSE TEXT FOR SELECT * FROM blue.mv1;
Explained Query (fast path):
  ReadIndex on=materialize.blue.mv1 mv1_primary_idx=[*** full scan ***]

Used Indexes:
  - materialize.blue.mv1_primary_idx (*** full scan ***)

Target cluster: blue_serving
