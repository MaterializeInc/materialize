# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# Test ALTER TABLE -- tables that are created after the source are irrelevant
#

> CREATE SECRET pgpass AS 'postgres'
> CREATE CONNECTION pg TO POSTGRES (
    HOST postgres,
    DATABASE postgres,
    USER postgres,
    PASSWORD SECRET pgpass
  )

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER USER postgres WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
DROP PUBLICATION IF EXISTS mz_source;

CREATE SCHEMA public;

CREATE TABLE base_table (f1 INTEGER);
ALTER TABLE base_table REPLICA IDENTITY FULL;
INSERT INTO base_table VALUES (1);

CREATE PUBLICATION mz_source FOR ALL TABLES;

> CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source')
  FOR ALL TABLES;

> SELECT * FROM base_table;
1

# Create "irrelevant" table after the materialized source took a snapshot of the publication
$ postgres-execute connection=postgres://postgres:postgres@postgres
CREATE TABLE irrelevant_table (f1 INTEGER);
ALTER TABLE irrelevant_table REPLICA IDENTITY FULL;
INSERT INTO irrelevant_table VALUES (1);
INSERT INTO base_table VALUES (2);

# A new table arriving does not prevent queries on existing views for this materialized source
> SELECT * FROM base_table;
1
2

# Alter the irrelevant table and insert a row to force a second relation message that would be incompatible
$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE irrelevant_table ADD COLUMN f2 varchar(2);
ALTER TABLE irrelevant_table DROP COLUMN f1;
INSERT INTO irrelevant_table VALUES ('ab');

# Query still works because the relation was ignored for being irrelevant
> SELECT * FROM base_table;
1
2

# Recreate the source and views to verify the irrelevant_table is part of the publication
> DROP SOURCE mz_source;
> CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source')
  FOR ALL TABLES;

# Check the first view still works
> SELECT * FROM base_table;
1
2

# Confirm the second table now has a corresponding view and it has the expected data
> SELECT * FROM irrelevant_table
<null>
ab

# Alter the irrelevant_table now that it is relevant
$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE irrelevant_table ADD COLUMN f3 char(2);
INSERT INTO irrelevant_table VALUES ('bc', 'de');

> SELECT * FROM base_table;
1
2

> SELECT * FROM irrelevant_table
<null>
ab
bc

# Alter in an incompatible way and ensure replication error does not occur
$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE irrelevant_table DROP COLUMN f2;
INSERT INTO irrelevant_table VALUES ('gh');

> SELECT * FROM base_table;
1
2
