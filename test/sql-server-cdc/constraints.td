# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ sql-server-connect name=sql-server
server=tcp:sql-server,1433;IntegratedSecurity=true;TrustServerCertificate=true;User ID=${arg.default-sql-server-user};Password=${arg.default-sql-server-password};Database=test

> CREATE SECRET IF NOT EXISTS sql_server_pass AS '${arg.default-sql-server-password}'

> DROP CONNECTION IF EXISTS sql_server_test_connection CASCADE
> CREATE CONNECTION sql_server_test_connection TO SQL SERVER (
    HOST 'sql-server',
    PORT 1433,
    DATABASE test,
    USER '${arg.default-sql-server-user}',
    PASSWORD = SECRET sql_server_pass
  );

> VALIDATE CONNECTION sql_server_test_connection;

$ sql-server-execute name=sql-server
CREATE TABLE pk (f1 INT, f2 INT, f3 INT, PRIMARY KEY (f1, f2));
INSERT INTO pk VALUES (1,1,null);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'pk', @role_name = 'SA', @supports_net_changes = 0;

# Only the constraint backing the capture instance's index is recorded in Materialize. SQL Server
# rejects dropping that index while the capture instance exists, but any other PRIMARY KEY or
# UNIQUE constraint can be dropped at any time, after which the upstream data may violate it.
CREATE TABLE unique_not_null (f1 INT NOT NULL, f2 INT NOT NULL, f3 INT, CONSTRAINT unique_not_null_key UNIQUE (f1, f2));
INSERT INTO unique_not_null VALUES (1,1,null);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'unique_not_null', @role_name = 'SA', @supports_net_changes = 0, @index_name = 'unique_not_null_key';

# A unique index over nullable columns cannot be the capture instance's index (SQL Server error
# 22838), so the capture instance has no index and no constraint is recorded.
CREATE TABLE unique_nullable (f1 INT, f2 INT, f3 INT, UNIQUE (f1, f2));
INSERT INTO unique_nullable VALUES (1,1,null), (2,2,null), (null,null,null);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'unique_nullable', @role_name = 'SA', @supports_net_changes = 0;

# The capture instance defaults to the primary key, so the UNIQUE constraint on f2 is not recorded.
CREATE TABLE pk_unique_overlap (f1 INT, f2 INT NOT NULL UNIQUE, PRIMARY KEY (f1, f2));
INSERT INTO pk_unique_overlap VALUES (1,1), (1,2), (2,3);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'pk_unique_overlap', @role_name = 'SA', @supports_net_changes = 0;

> BEGIN;
> CREATE SOURCE mz_source FROM SQL SERVER CONNECTION sql_server_test_connection;
> CREATE TABLE pk FROM SOURCE mz_source (REFERENCE pk);
> CREATE TABLE unique_not_null FROM SOURCE mz_source (REFERENCE unique_not_null);
> CREATE TABLE unique_nullable FROM SOURCE mz_source (REFERENCE unique_nullable);
> CREATE TABLE pk_unique_overlap FROM SOURCE mz_source (REFERENCE pk_unique_overlap);
> COMMIT;


> CREATE DEFAULT INDEX ON pk;
> CREATE DEFAULT INDEX ON unique_not_null;
> CREATE DEFAULT INDEX ON unique_nullable;
> CREATE DEFAULT INDEX ON pk_unique_overlap;

> SELECT key FROM (SHOW INDEXES ON pk);
{f1,f2}

> SELECT key FROM (SHOW INDEXES ON unique_not_null);
{f1,f2}

> SELECT key FROM (SHOW INDEXES ON unique_nullable);
{f1,f2,f3}

> SELECT key FROM (SHOW INDEXES ON pk_unique_overlap);
{f1,f2}

> SELECT create_sql LIKE '%PRIMARY KEY ("f1", "f2")%', create_sql LIKE '%UNIQUE%' FROM mz_tables WHERE name = 'pk_unique_overlap';
true false

> SELECT create_sql LIKE '%CONSTRAINT "unique_not_null_key" UNIQUE NULLS NOT DISTINCT ("f1", "f2")%' FROM mz_tables WHERE name = 'unique_not_null';
true

> SELECT create_sql LIKE '%UNIQUE%' OR create_sql LIKE '%PRIMARY KEY%' FROM mz_tables WHERE name = 'unique_nullable';
false

> SELECT * FROM unique_nullable
1 1 <null>
2 2 <null>
<null> <null> <null>

> SELECT * FROM pk_unique_overlap;
1 1
1 2
2 3

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


# Unique converted to keys
? EXPLAIN OPTIMIZED PLAN AS VERBOSE TEXT FOR SELECT DISTINCT f1, f2 FROM unique_not_null
Explained Query (fast path):
  Project (#0, #1)
    ReadIndex on=materialize.public.unique_not_null unique_not_null_primary_idx=[*** full scan ***]

Used Indexes:
  - materialize.public.unique_not_null_primary_idx (*** full scan ***)

Target cluster: quickstart


# No key recorded, so the DISTINCT is kept.
? EXPLAIN OPTIMIZED PLAN AS VERBOSE TEXT FOR SELECT DISTINCT f1, f2 FROM unique_nullable
Explained Query:
  Distinct project=[#0, #1]
    Project (#0, #1)
      ReadIndex on=unique_nullable unique_nullable_primary_idx=[*** full scan ***]

Used Indexes:
  - materialize.public.unique_nullable_primary_idx (*** full scan ***)

Target cluster: quickstart


? EXPLAIN OPTIMIZED PLAN AS VERBOSE TEXT FOR SELECT DISTINCT f1, f2 FROM pk_unique_overlap
Explained Query (fast path):
  ReadIndex on=materialize.public.pk_unique_overlap pk_unique_overlap_primary_idx=[*** full scan ***]

Used Indexes:
  - materialize.public.pk_unique_overlap_primary_idx (*** full scan ***)

Target cluster: quickstart


#
# An unreferenced UNIQUE constraint is not a key. SQL Server lets it be dropped at any time, and a
# recorded key would let the optimizer drop the DISTINCT and return the duplicate rows.

$ sql-server-execute name=sql-server
CREATE TABLE unref_unique (id INT NOT NULL CONSTRAINT unref_unique_pk PRIMARY KEY, code INT NOT NULL CONSTRAINT unref_unique_code UNIQUE, val INT NOT NULL);
INSERT INTO unref_unique VALUES (1, 10, 100), (2, 20, 200);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'unref_unique', @role_name = 'SA', @supports_net_changes = 0;

> CREATE TABLE unref_unique FROM SOURCE mz_source (REFERENCE unref_unique);

> SELECT create_sql LIKE '%PRIMARY KEY ("id")%', create_sql LIKE '%UNIQUE%' FROM mz_tables WHERE name = 'unref_unique';
true false

$ sql-server-execute name=sql-server
ALTER TABLE unref_unique DROP CONSTRAINT unref_unique_code;
INSERT INTO unref_unique VALUES (3, 10, 300);

> SELECT DISTINCT code FROM unref_unique;
10
20

> SELECT code, count(*) FROM unref_unique GROUP BY code;
10 2
20 1

# Adding a UNIQUE constraint after the source exists does not add a key either.
$ sql-server-execute name=sql-server
ALTER TABLE unref_unique ADD CONSTRAINT unref_unique_val UNIQUE (val);
INSERT INTO unref_unique VALUES (4, 40, 400);
ALTER TABLE unref_unique DROP CONSTRAINT unref_unique_val;
INSERT INTO unref_unique VALUES (5, 50, 400);

> SELECT DISTINCT val FROM unref_unique;
100
200
300
400

> SELECT create_sql LIKE '%UNIQUE%' FROM mz_tables WHERE name = 'unref_unique';
false

#
# A capture instance created with @index_name uses that UNIQUE constraint as its index. The
# primary key is then unprotected and must not be recorded.

$ sql-server-execute name=sql-server
CREATE TABLE ref_unique (id INT NOT NULL CONSTRAINT ref_unique_pk PRIMARY KEY, code INT NOT NULL CONSTRAINT ref_unique_code UNIQUE, val INT);
INSERT INTO ref_unique VALUES (1, 10, 100), (2, 20, 200);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'ref_unique', @role_name = 'SA', @supports_net_changes = 0, @index_name = 'ref_unique_code';

> CREATE TABLE ref_unique FROM SOURCE mz_source (REFERENCE ref_unique);

> SELECT create_sql LIKE '%CONSTRAINT "ref_unique_code" UNIQUE NULLS NOT DISTINCT ("code")%', create_sql LIKE '%PRIMARY KEY%' FROM mz_tables WHERE name = 'ref_unique';
true false

# SQL Server rejects dropping the index the capture instance uses (error 22979) ...
$ sql-server-set-from-sql name=sql-server var=drop-referenced-outcome
BEGIN TRY
  ALTER TABLE ref_unique DROP CONSTRAINT ref_unique_code;
  SELECT 'dropped';
END TRY
BEGIN CATCH
  SELECT CAST(ERROR_NUMBER() AS VARCHAR(10));
END CATCH

> SELECT '${drop-referenced-outcome}';
22979

# ... but the primary key can go.
$ sql-server-execute name=sql-server
ALTER TABLE ref_unique DROP CONSTRAINT ref_unique_pk;
INSERT INTO ref_unique VALUES (1, 30, 300);

> SELECT DISTINCT id FROM ref_unique;
1
2

> SELECT id, count(*) FROM ref_unique GROUP BY id;
1 2
2 1

> SELECT DISTINCT code FROM ref_unique;
10
20
30

#
# SQL Server only protects the index of the newest capture instance for a table. Once a second
# capture instance with a different index exists, the index of the capture instance Materialize
# replicates can be dropped, and the recorded key no longer holds. The DISTINCT below returns
# duplicates because the optimizer removes it based on the stale key. This records a known
# limitation, not desired behavior.

$ sql-server-execute name=sql-server
CREATE TABLE superseded (id INT NOT NULL CONSTRAINT superseded_pk PRIMARY KEY, code INT NOT NULL CONSTRAINT superseded_code UNIQUE, val INT);
INSERT INTO superseded VALUES (1, 10, 100), (2, 20, 200);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'superseded', @role_name = 'SA', @supports_net_changes = 0, @capture_instance = 'superseded_one';

> CREATE TABLE superseded FROM SOURCE mz_source (REFERENCE superseded);

> SELECT create_sql LIKE '%PRIMARY KEY ("id")%', create_sql LIKE '%UNIQUE%' FROM mz_tables WHERE name = 'superseded';
true false

> SELECT * FROM superseded;
1 10 100
2 20 200

# While superseded_one is the only capture instance, SQL Server rejects dropping its index ...
$ sql-server-set-from-sql name=sql-server var=drop-sole-pk-outcome
BEGIN TRY
  ALTER TABLE superseded DROP CONSTRAINT superseded_pk;
  SELECT 'dropped';
END TRY
BEGIN CATCH
  SELECT CAST(ERROR_NUMBER() AS VARCHAR(10));
END CATCH

> SELECT '${drop-sole-pk-outcome}';
22979

# ... but a newer capture instance with a different index lifts that protection.
$ sql-server-execute name=sql-server
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'superseded', @role_name = 'SA', @supports_net_changes = 0, @capture_instance = 'superseded_two', @index_name = 'superseded_code';
ALTER TABLE superseded DROP CONSTRAINT superseded_pk;
INSERT INTO superseded VALUES (1, 30, 300);

> SELECT * FROM superseded;
1 10 100
1 30 300
2 20 200

> SELECT DISTINCT id FROM superseded;
1
1
2
