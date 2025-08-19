# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# IMPORTANT: The Postgres server has a custom pg_hba.conf that only
# accepts connections from specific users. You will have to update
# pg_hba.conf if you modify the existing user names or add new ones.

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
CREATE SCHEMA public;

DROP PUBLICATION IF EXISTS mz_source;
CREATE PUBLICATION mz_source FOR ALL TABLES;

CREATE TABLE pk (f1 INT, f2 INT, f3 INT, PRIMARY KEY (f1, f2));
INSERT INTO pk VALUES (1,1,null);
ALTER TABLE pk REPLICA IDENTITY FULL;

CREATE TABLE unique_not_null (f1 INT NOT NULL, f2 INT NOT NULL, f3 INT, UNIQUE (f1, f2));
INSERT INTO unique_not_null VALUES (1,1,null);
ALTER TABLE unique_not_null REPLICA IDENTITY FULL;

CREATE TABLE unique_nullable (f1 INT, f2 INT, f3 INT, UNIQUE (f1, f2));
INSERT INTO unique_nullable VALUES (1,1,null), (null,null,null), (null,null,null);
ALTER TABLE unique_nullable REPLICA IDENTITY FULL;

> BEGIN
> CREATE SOURCE mz_source FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source');

> CREATE TABLE pk FROM SOURCE mz_source (REFERENCE pk);
> CREATE TABLE unique_not_null FROM SOURCE mz_source (REFERENCE unique_not_null);
> CREATE TABLE unique_nullable FROM SOURCE mz_source (REFERENCE unique_nullable);
> COMMIT

> CREATE DEFAULT INDEX ON pk;
> CREATE DEFAULT INDEX ON unique_not_null;
> CREATE DEFAULT INDEX ON unique_nullable;

> SELECT key FROM (SHOW INDEXES ON pk);
{f1,f2}

> SELECT key FROM (SHOW INDEXES ON unique_not_null);
{f1,f2}

> SELECT key FROM (SHOW INDEXES ON unique_nullable);
{f1,f2,f3}

> SELECT * FROM unique_nullable
1 1 <null>
<null> <null> <null>
<null> <null> <null>

# Not null constraint optimizes like this:
? EXPLAIN OPTIMIZED PLAN AS VERBOSE TEXT FOR SELECT * FROM pk WHERE f1 IS NULL OR f2 IS NULL;
Explained Query (fast path):
  Constant <empty>

Target cluster: quickstart

? EXPLAIN OPTIMIZED PLAN AS VERBOSE TEXT FOR SELECT * FROM unique_not_null WHERE f1 IS NULL OR f2 IS NULL;
Explained Query (fast path):
  Constant <empty>

Target cluster: quickstart

#
# Unique converted to keys
? EXPLAIN OPTIMIZED PLAN AS VERBOSE TEXT FOR SELECT DISTINCT f1, f2 FROM pk
Explained Query (fast path):
  Project (#0, #1)
    ReadIndex on=materialize.public.pk pk_primary_idx=[*** full scan ***]

Used Indexes:
  - materialize.public.pk_primary_idx (*** full scan ***)

Target cluster: quickstart

#
# Unique converted to keys
? EXPLAIN OPTIMIZED PLAN AS VERBOSE TEXT FOR SELECT DISTINCT f1, f2 FROM unique_not_null
Explained Query (fast path):
  Project (#0, #1)
    ReadIndex on=materialize.public.unique_not_null unique_not_null_primary_idx=[*** full scan ***]

Used Indexes:
  - materialize.public.unique_not_null_primary_idx (*** full scan ***)

Target cluster: quickstart

#
# Unique NOT converted to keys because values are nullable
? EXPLAIN OPTIMIZED PLAN AS VERBOSE TEXT FOR SELECT DISTINCT f1, f2 FROM unique_nullable
Explained Query:
  Distinct project=[#0, #1]
    Project (#0, #1)
      ReadIndex on=unique_nullable unique_nullable_primary_idx=[*** full scan ***]

Used Indexes:
  - materialize.public.unique_nullable_primary_idx (*** full scan ***)

Target cluster: quickstart
