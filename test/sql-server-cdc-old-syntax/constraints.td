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

CREATE TABLE unique_not_null (f1 INT NOT NULL, f2 INT NOT NULL, f3 INT, UNIQUE (f1, f2));
INSERT INTO unique_not_null VALUES (1,1,null);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'unique_not_null', @role_name = 'SA', @supports_net_changes = 0;

CREATE TABLE unique_nullable (f1 INT, f2 INT, f3 INT, UNIQUE (f1, f2));
INSERT INTO unique_nullable VALUES (1,1,null), (2,2,null), (null,null,null);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'unique_nullable', @role_name = 'SA', @supports_net_changes = 0;

CREATE TABLE pk_unique_overlap (f1 INT, f2 INT NOT NULL UNIQUE, PRIMARY KEY (f1, f2));
INSERT INTO pk_unique_overlap VALUES (1,1), (1,2), (2,3);
EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'pk_unique_overlap', @role_name = 'SA', @supports_net_changes = 0;

> CREATE SOURCE mz_source
  FROM SQL SERVER CONNECTION sql_server_test_connection
  FOR ALL TABLES;


> CREATE DEFAULT INDEX ON pk;
> CREATE DEFAULT INDEX ON unique_not_null;
> CREATE DEFAULT INDEX ON unique_nullable;
> CREATE DEFAULT INDEX ON pk_unique_overlap;

> SELECT key FROM (SHOW INDEXES ON pk);
{f1,f2}

> SELECT key FROM (SHOW INDEXES ON unique_not_null);
{f1,f2}

# in SQL Server NULLS are not distinct
# see https://learn.microsoft.com/en-us/sql/relational-databases/tables/unique-constraints-and-check-constraints?view=sql-server-ver17#unique-constraints
> SELECT key FROM (SHOW INDEXES ON unique_nullable);
{f1,f2}

> SELECT key FROM (SHOW INDEXES ON pk_unique_overlap);
{f1,f2}

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


# Unique NOT converted to keys because values are nullable
? EXPLAIN OPTIMIZED PLAN AS VERBOSE TEXT FOR SELECT DISTINCT f1, f2 FROM unique_nullable
Explained Query (fast path):
  Project (#0, #1)
    ReadIndex on=materialize.public.unique_nullable unique_nullable_primary_idx=[*** full scan ***]

Used Indexes:
  - materialize.public.unique_nullable_primary_idx (*** full scan ***)

Target cluster: quickstart


? EXPLAIN OPTIMIZED PLAN AS VERBOSE TEXT FOR SELECT DISTINCT f1, f2 FROM pk_unique_overlap
Explained Query (fast path):
  ReadIndex on=materialize.public.pk_unique_overlap pk_unique_overlap_primary_idx=[*** full scan ***]

Used Indexes:
  - materialize.public.pk_unique_overlap_primary_idx (*** full scan ***)

Target cluster: quickstart
