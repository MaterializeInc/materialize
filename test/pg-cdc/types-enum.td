# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# ENUM supported only as TEXT
#

> CREATE SECRET pgpass AS 'postgres'
> CREATE CONNECTION pg TO POSTGRES (
    HOST postgres,
    DATABASE postgres,
    USER postgres,
    PASSWORD SECRET pgpass
  )


# Insert data pre-snapshot
$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER USER postgres WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
DROP PUBLICATION IF EXISTS mz_source;

CREATE SCHEMA public;

CREATE TYPE enum1 AS ENUM ('val1', 'val2');
CREATE TABLE enum_type ( f1 enum1);
INSERT INTO enum_type VALUES ('val1'), ('val2');
ALTER TABLE enum_type REPLICA IDENTITY FULL;

CREATE PUBLICATION mz_source FOR ALL TABLES;

> CREATE SOURCE mz_source FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source');

> CREATE TABLE enum_type FROM SOURCE mz_source (REFERENCE enum_type) WITH (TEXT COLUMNS (f1));

> SELECT COUNT(*) > 0 FROM enum_type;
true

# Insert the same data post-snapshot
$ postgres-execute connection=postgres://postgres:postgres@postgres
INSERT INTO enum_type SELECT * FROM enum_type;

> SELECT pg_typeof(f1) FROM enum_type LIMIT 1;
text

> SELECT * FROM enum_type;
val1
val1
val2
val2
