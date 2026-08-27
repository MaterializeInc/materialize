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
# a valid name to exclude, and excluding a key column alone also prunes the key.
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
contains:UNIQUE constraint "stall_uq" (wallet) on source table public.stall_tbl was dropped or altered upstream

! SELECT * FROM stall_tbl;
contains:EXCLUDE CONSTRAINTS ('stall_uq')

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
