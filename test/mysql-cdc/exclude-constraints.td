# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

#
# EXCLUDE CONSTRAINTS / EXCLUDE ALL CONSTRAINTS: excluded upstream constraints
# (unique indexes; the primary key's index is named PRIMARY) are not recorded
# as Materialize keys, so dropping them upstream is a non-event instead of
# stalling the table.
#

$ set-sql-timeout duration=60s

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_exclude_constraints_option = true

> CREATE SECRET mysqlpass AS '${arg.mysql-root-password}'

> CREATE CONNECTION mysqc TO MYSQL (
    HOST mysql,
    USER root,
    PASSWORD SECRET mysqlpass
  )

$ mysql-connect name=mysql url=mysql://root@mysql password=${arg.mysql-root-password}

$ mysql-execute name=mysql
DROP DATABASE IF EXISTS public;
CREATE DATABASE public;
USE public;

CREATE TABLE uniq_excl (id INT PRIMARY KEY, wallet VARCHAR(64) NOT NULL, UNIQUE KEY uq_wallet (wallet));
INSERT INTO uniq_excl VALUES (1, 'a'), (2, 'b');

CREATE TABLE pk_excl (id INT PRIMARY KEY, val VARCHAR(64) NOT NULL);
INSERT INTO pk_excl VALUES (1, 'a'), (2, 'b');

CREATE TABLE all_excl (id INT PRIMARY KEY, email VARCHAR(64) NOT NULL, val INT NOT NULL);
INSERT INTO all_excl VALUES (1, 'a@b.c', 10);

CREATE TABLE stall_tbl (id INT PRIMARY KEY, wallet VARCHAR(64) NOT NULL, UNIQUE KEY stall_uq (wallet));
INSERT INTO stall_tbl VALUES (1, 'a');

CREATE TABLE excl_combo (id INT PRIMARY KEY, u INT NOT NULL, val VARCHAR(64) NOT NULL, UNIQUE KEY uq_u (u));
INSERT INTO excl_combo VALUES (1, 10, 'a');

CREATE TABLE excl_not_null (id INT PRIMARY KEY, u INT NOT NULL, val VARCHAR(64) NOT NULL, UNIQUE KEY unq_u (u));
INSERT INTO excl_not_null VALUES (1, 10, 'a');

> CREATE SOURCE mz_source FROM MYSQL CONNECTION mysqc;

#
# Validation errors
#

! CREATE TABLE uniq_excl FROM SOURCE mz_source (REFERENCE public.uniq_excl)
  WITH (EXCLUDE CONSTRAINTS ('nope'));
contains:EXCLUDE CONSTRAINTS refers to constraints that do not exist on table public.uniq_excl

# Constraint names match case-sensitively.
! CREATE TABLE uniq_excl FROM SOURCE mz_source (REFERENCE public.uniq_excl)
  WITH (EXCLUDE CONSTRAINTS ('UQ_WALLET'));
contains:EXCLUDE CONSTRAINTS refers to constraints that do not exist on table public.uniq_excl

! CREATE TABLE uniq_excl FROM SOURCE mz_source (REFERENCE public.uniq_excl)
  WITH (EXCLUDE CONSTRAINTS ('uq_wallet'), EXCLUDE ALL CONSTRAINTS);
contains:EXCLUDE ALL CONSTRAINTS cannot be combined with EXCLUDE CONSTRAINTS

#
# Excluding a named unique index: the PRIMARY KEY survives, and the excluded
# index's later upstream drop is a non-event.
#

> CREATE TABLE uniq_excl FROM SOURCE mz_source (REFERENCE public.uniq_excl)
  WITH (EXCLUDE CONSTRAINTS ('uq_wallet'));

> SELECT * FROM uniq_excl;
1 a
2 b

> CREATE DEFAULT INDEX ON uniq_excl;
> SELECT key FROM (SHOW INDEXES ON uniq_excl);
{id}

> SELECT create_sql LIKE '%EXCLUDE CONSTRAINTS%uq_wallet%' FROM (SHOW CREATE TABLE uniq_excl);
true

$ mysql-execute name=mysql
USE public;
ALTER TABLE uniq_excl DROP INDEX uq_wallet;
INSERT INTO uniq_excl VALUES (3, 'c');

> SELECT * FROM uniq_excl;
1 a
2 b
3 c

#
# Excluding the primary key by its MySQL index name PRIMARY yields a keyless
# table; dropping the PK upstream is a non-event.
#

> CREATE TABLE pk_excl FROM SOURCE mz_source (REFERENCE public.pk_excl)
  WITH (EXCLUDE CONSTRAINTS ('PRIMARY'));

> CREATE DEFAULT INDEX ON pk_excl;
> SELECT key FROM (SHOW INDEXES ON pk_excl);
{id,val}

$ mysql-execute name=mysql
USE public;
ALTER TABLE pk_excl DROP PRIMARY KEY;
INSERT INTO pk_excl VALUES (3, 'c');

> SELECT * FROM pk_excl;
1 a
2 b
3 c

#
# EXCLUDE ALL CONSTRAINTS: no keys, every column nullable, so DROP PRIMARY KEY
# and DROP NOT NULL are both non-events.
#

> CREATE TABLE all_excl FROM SOURCE mz_source (REFERENCE public.all_excl)
  WITH (EXCLUDE ALL CONSTRAINTS);

> CREATE DEFAULT INDEX ON all_excl;
> SELECT key FROM (SHOW INDEXES ON all_excl);
{id,email,val}

> SELECT name, nullable FROM mz_columns WHERE id = (SELECT id FROM mz_tables WHERE name = 'all_excl');
id true
email true
val true

$ mysql-execute name=mysql
USE public;
ALTER TABLE all_excl DROP PRIMARY KEY;
ALTER TABLE all_excl MODIFY email VARCHAR(64) NULL;
INSERT INTO all_excl VALUES (2, NULL, 20);

> SELECT id, val FROM all_excl WHERE email IS NULL;
2 20

> SELECT * FROM all_excl;
1 a@b.c 10
2 <null> 20

#
# An empty exclusion list behaves as if the option were omitted.
#

> CREATE TABLE uniq_excl_empty FROM SOURCE mz_source (REFERENCE public.stall_tbl)
  WITH (EXCLUDE CONSTRAINTS ());

> CREATE DEFAULT INDEX ON uniq_excl_empty;
> SELECT key FROM (SHOW INDEXES ON uniq_excl_empty);
{id}

> DROP TABLE uniq_excl_empty;

#
# Combining with EXCLUDE COLUMNS: a key whose column is also excluded is still
# a valid name to exclude.
#

> CREATE TABLE excl_combo FROM SOURCE mz_source (REFERENCE public.excl_combo)
  WITH (EXCLUDE COLUMNS (u), EXCLUDE CONSTRAINTS ('uq_u'));

> SELECT * FROM excl_combo;
1 a

#
# A non-excluded unique index dropped upstream still stalls, now with an error
# that names the index and the recovery workflow.
#

> CREATE TABLE stall_tbl FROM SOURCE mz_source (REFERENCE public.stall_tbl);

> SELECT * FROM stall_tbl;
1 a

$ mysql-execute name=mysql
USE public;
ALTER TABLE stall_tbl DROP INDEX stall_uq;
INSERT INTO stall_tbl VALUES (2, 'b');

! SELECT * FROM stall_tbl;
contains:incompatible schema change on public.stall_tbl: UNIQUE constraint "stall_uq" (wallet) was dropped upstream
hint:WITH (EXCLUDE CONSTRAINTS ('stall_uq')) before the upstream drop

# Recovery: a replacement table snapshots the current upstream schema, which no
# longer has the index, so no exclusion is needed. Then drop the errored table.
> CREATE TABLE stall_tbl_v2 FROM SOURCE mz_source (REFERENCE public.stall_tbl);

> SELECT * FROM stall_tbl_v2;
1 a
2 b

> DROP TABLE stall_tbl;

#
# Feature flag: the options are rejected when disabled.
#

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_exclude_constraints_option = false

! CREATE TABLE uniq_excl_flagged FROM SOURCE mz_source (REFERENCE public.uniq_excl)
  WITH (EXCLUDE ALL CONSTRAINTS);
contains:not available

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_exclude_constraints_option = true

#
# EXCLUDE ALL NOT NULL CONSTRAINTS: keys and uniqueness remain, but every column nullable, so
# DROP NOT NULL is a non-event.
#

> CREATE TABLE all_nullable
  FROM SOURCE mz_source (REFERENCE public.excl_not_null)
  WITH (EXCLUDE ALL NOT NULL CONSTRAINTS);

> SELECT create_sql LIKE '%EXCLUDE ALL NOT NULL CONSTRAINTS%' FROM (SHOW CREATE TABLE all_nullable);
true

> CREATE DEFAULT INDEX ON all_nullable;
> SELECT key FROM (SHOW INDEXES ON all_nullable);
{id}

# All columns except id should be nullable, but id is a PK and cannot be NULL
> SELECT name, nullable FROM mz_columns WHERE id = (SELECT id FROM mz_tables WHERE name = 'all_nullable');
id false
u true
val true

> CREATE TABLE all_nullable_combo
  FROM SOURCE mz_source (REFERENCE public.excl_not_null)
  WITH (EXCLUDE ALL NOT NULL CONSTRAINTS, EXCLUDE CONSTRAINTS ('PRIMARY'));

> CREATE DEFAULT INDEX ON all_nullable_combo;
> SELECT key FROM (SHOW INDEXES ON all_nullable_combo);
{id,u,val}

> SELECT name, nullable FROM mz_columns WHERE id = (SELECT id FROM mz_tables WHERE name = 'all_nullable_combo');
id true
u true
val true

$ mysql-execute name=mysql
USE public;
ALTER TABLE excl_not_null MODIFY val VARCHAR(64) NULL;
INSERT INTO excl_not_null VALUES (2, 20, NULL);

> SELECT * FROM all_nullable;
1 10 a
2 20 <null>

> SELECT * FROM all_nullable_combo;
1 10 a
2 20 <null>

$ mysql-execute name=mysql
USE public;
ALTER TABLE excl_not_null DROP PRIMARY KEY;
INSERT INTO excl_not_null VALUES (3, 30, 'c');

! SELECT * FROM all_nullable;
contains:incompatible schema change on public.excl_not_null: PRIMARY KEY constraint "PRIMARY" (id) was dropped upstream
hint:WITH (EXCLUDE CONSTRAINTS ('PRIMARY')) before the upstream drop

> DROP TABLE all_nullable;

> SELECT * FROM all_nullable_combo;
1 10 a
2 20 <null>
3 30 c

$ mysql-execute name=mysql
USE public;
ALTER TABLE excl_not_null MODIFY u INT NULL;
INSERT INTO excl_not_null VALUES (4, NULL, 'd');
INSERT INTO excl_not_null VALUES (5, NULL, 'e');

> SELECT * FROM all_nullable_combo;
1 10 a
2 20 <null>
3 30 c
4 <null> d
5 <null> e

> SELECT u, COUNT(*) FROM all_nullable_combo GROUP BY u;
10 1
20 1
30 1
<null> 2

# The unique constraint still needs to be explicitly excluded if you want to drop the constraint.
$ mysql-execute name=mysql
USE public;
ALTER TABLE excl_not_null DROP INDEX unq_u;
INSERT INTO excl_not_null VALUES (6, 60, 'f');

! SELECT * FROM all_nullable_combo;
contains:incompatible schema change on public.excl_not_null: UNIQUE constraint "unq_u" (u) was dropped upstream
hint:WITH (EXCLUDE CONSTRAINTS ('unq_u')) before the upstream drop

> DROP TABLE all_nullable_combo;

#
# A retained primary key keeps its columns NOT NULL. A NULL that slips past
# schema verification, because the key was dropped and restored while the
# source had no replica, is a decode error that the later UPDATE retracts, not
# a NULL stored in a non-nullable column.
#

$ mysql-execute name=mysql
USE public;
CREATE TABLE pk_gap (id INT PRIMARY KEY, v INT NOT NULL);
INSERT INTO pk_gap VALUES (1, 10);

> CREATE CLUSTER pk_gap_cluster SIZE '${arg.default-replica-size}';
> CREATE SOURCE pk_gap_source IN CLUSTER pk_gap_cluster FROM MYSQL CONNECTION mysqc;
> CREATE TABLE pk_gap FROM SOURCE pk_gap_source (REFERENCE public.pk_gap)
  WITH (EXCLUDE ALL NOT NULL CONSTRAINTS);

> SELECT * FROM pk_gap;
1 10

> ALTER CLUSTER pk_gap_cluster SET (REPLICATION FACTOR 0)

$ mysql-execute name=mysql
USE public;
ALTER TABLE pk_gap DROP PRIMARY KEY, MODIFY id INT NULL;
INSERT INTO pk_gap VALUES (NULL, 20);
UPDATE pk_gap SET id = 2 WHERE id IS NULL;
ALTER TABLE pk_gap MODIFY id INT NOT NULL, ADD PRIMARY KEY (id);

> ALTER CLUSTER pk_gap_cluster SET (REPLICATION FACTOR 1)

> SELECT * FROM pk_gap;
1 10
2 20

> DROP SOURCE pk_gap_source CASCADE;
> DROP CLUSTER pk_gap_cluster;
