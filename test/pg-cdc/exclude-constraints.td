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
# are not recorded as Materialize keys, so dropping them upstream is a
# non-event instead of stalling the table.
#

$ set-sql-timeout duration=60s

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_exclude_constraints_option = true
ALTER SYSTEM SET pg_schema_validation_interval = '2s'

> CREATE CLUSTER storage SIZE '${arg.default-replica-size}'

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

CREATE TABLE uniq_excl (id int PRIMARY KEY, wallet text NOT NULL, CONSTRAINT uq_wallet UNIQUE (wallet));
INSERT INTO uniq_excl VALUES (1, 'a'), (2, 'b');
ALTER TABLE uniq_excl REPLICA IDENTITY FULL;

CREATE TABLE pk_excl (id int PRIMARY KEY, val text NOT NULL);
INSERT INTO pk_excl VALUES (1, 'a'), (2, 'b');
ALTER TABLE pk_excl REPLICA IDENTITY FULL;

CREATE TABLE all_excl (id int PRIMARY KEY, email text NOT NULL, val int NOT NULL);
INSERT INTO all_excl VALUES (1, 'a@b.c', 10);
ALTER TABLE all_excl REPLICA IDENTITY FULL;

CREATE TABLE stall_tbl (id int PRIMARY KEY, wallet text NOT NULL, CONSTRAINT stall_uq UNIQUE (wallet));
INSERT INTO stall_tbl VALUES (1, 'a');
ALTER TABLE stall_tbl REPLICA IDENTITY FULL;

CREATE TABLE excl_combo (id int PRIMARY KEY, u int NOT NULL, val text NOT NULL, CONSTRAINT uq_u UNIQUE (u));
INSERT INTO excl_combo VALUES (1, 10, 'a');
ALTER TABLE excl_combo REPLICA IDENTITY FULL;

ANALYZE;

> CREATE SOURCE mz_source
  IN CLUSTER storage
  FROM POSTGRES CONNECTION pg (PUBLICATION mz_source);

#
# Validation errors
#

# Naming a constraint that does not exist on the table errors.
! CREATE TABLE uniq_excl
  FROM SOURCE mz_source (REFERENCE public.uniq_excl)
  WITH (EXCLUDE CONSTRAINTS ('nope'));
contains:EXCLUDE CONSTRAINTS refers to constraints that do not exist on table public.uniq_excl

# Constraint names match case-sensitively.
! CREATE TABLE uniq_excl
  FROM SOURCE mz_source (REFERENCE public.uniq_excl)
  WITH (EXCLUDE CONSTRAINTS ('UQ_WALLET'));
contains:EXCLUDE CONSTRAINTS refers to constraints that do not exist on table public.uniq_excl

# The two options cannot be combined.
! CREATE TABLE uniq_excl
  FROM SOURCE mz_source (REFERENCE public.uniq_excl)
  WITH (EXCLUDE CONSTRAINTS ('uq_wallet'), EXCLUDE ALL CONSTRAINTS);
contains:EXCLUDE ALL CONSTRAINTS cannot be combined with EXCLUDE CONSTRAINTS

#
# Excluding a named UNIQUE constraint: the PRIMARY KEY survives, the excluded
# constraint is not recorded, and its later upstream drop is a non-event.
#

> CREATE TABLE uniq_excl
  FROM SOURCE mz_source (REFERENCE public.uniq_excl)
  WITH (EXCLUDE CONSTRAINTS ('uq_wallet'));

> SELECT * FROM uniq_excl;
1 a
2 b

# Only the PRIMARY KEY remains a key.
> CREATE DEFAULT INDEX ON uniq_excl;
> SELECT key FROM (SHOW INDEXES ON uniq_excl);
{id}

# The option roundtrips through SHOW CREATE TABLE.
> SELECT create_sql LIKE '%EXCLUDE CONSTRAINTS%uq_wallet%' FROM (SHOW CREATE TABLE uniq_excl);
true

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE uniq_excl DROP CONSTRAINT uq_wallet;
INSERT INTO uniq_excl VALUES (3, 'c');

> SELECT * FROM uniq_excl;
1 a
2 b
3 c

#
# Excluding the PRIMARY KEY by name yields a keyless table; dropping the PK
# upstream is a non-event.
#

> CREATE TABLE pk_excl
  FROM SOURCE mz_source (REFERENCE public.pk_excl)
  WITH (EXCLUDE CONSTRAINTS ('pk_excl_pkey'));

# No keys: the default index falls back to all columns.
> CREATE DEFAULT INDEX ON pk_excl;
> SELECT key FROM (SHOW INDEXES ON pk_excl);
{id,val}

# Excluding the PK drops the key but not the columns' NOT NULL.
> SELECT name, nullable FROM mz_columns WHERE id = (SELECT id FROM mz_tables WHERE name = 'pk_excl');
id false
val false

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE pk_excl DROP CONSTRAINT pk_excl_pkey;
INSERT INTO pk_excl VALUES (3, 'c');

> SELECT * FROM pk_excl;
1 a
2 b
3 c

#
# EXCLUDE ALL CONSTRAINTS: no keys, every column nullable, so DROP PRIMARY KEY
# and DROP NOT NULL are both non-events.
#

> CREATE TABLE all_excl
  FROM SOURCE mz_source (REFERENCE public.all_excl)
  WITH (EXCLUDE ALL CONSTRAINTS);

> CREATE DEFAULT INDEX ON all_excl;
> SELECT key FROM (SHOW INDEXES ON all_excl);
{id,email,val}

> SELECT name, nullable FROM mz_columns WHERE id = (SELECT id FROM mz_tables WHERE name = 'all_excl');
id true
email true
val true

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE all_excl DROP CONSTRAINT all_excl_pkey;
ALTER TABLE all_excl ALTER COLUMN email DROP NOT NULL;
INSERT INTO all_excl VALUES (2, NULL, 20);

> SELECT id, val FROM all_excl WHERE email IS NULL;
2 20

> SELECT * FROM all_excl;
1 a@b.c 10
2 <null> 20

#
# An empty exclusion list behaves as if the option were omitted.
#

> CREATE TABLE uniq_excl_empty
  FROM SOURCE mz_source (REFERENCE public.stall_tbl)
  WITH (EXCLUDE CONSTRAINTS ());

> CREATE DEFAULT INDEX ON uniq_excl_empty;
> SELECT key FROM (SHOW INDEXES ON uniq_excl_empty);
{id}

> DROP TABLE uniq_excl_empty;

#
# Combining with EXCLUDE COLUMNS: a constraint whose columns are also excluded
# is still a valid name to exclude.
#

> CREATE TABLE excl_combo
  FROM SOURCE mz_source (REFERENCE public.excl_combo)
  WITH (EXCLUDE COLUMNS (u), EXCLUDE CONSTRAINTS ('uq_u'));

> SELECT * FROM excl_combo;
1 a

#
# A non-excluded constraint dropped upstream still stalls, now with an error
# that names the constraint and the recovery workflow.
#

> CREATE TABLE stall_tbl
  FROM SOURCE mz_source (REFERENCE public.stall_tbl);

> SELECT * FROM stall_tbl;
1 a

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE stall_tbl DROP CONSTRAINT stall_uq;

! SELECT * FROM stall_tbl;
contains:UNIQUE constraint "stall_uq" (wallet) on source table public.stall_tbl was dropped or altered upstream

! SELECT * FROM stall_tbl;
contains:EXCLUDE CONSTRAINTS ('stall_uq')

# Recovery: a replacement table snapshots the current upstream schema, which no
# longer has the constraint, so no exclusion is needed. Then drop the errored
# table.
$ postgres-execute connection=postgres://postgres:postgres@postgres
INSERT INTO stall_tbl VALUES (2, 'b');

> CREATE TABLE stall_tbl_v2
  FROM SOURCE mz_source (REFERENCE public.stall_tbl);

> SELECT * FROM stall_tbl_v2;
1 a
2 b

> DROP TABLE stall_tbl;

#
# Feature flag: the options are rejected when disabled.
#

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_exclude_constraints_option = false

! CREATE TABLE uniq_excl_flagged
  FROM SOURCE mz_source (REFERENCE public.uniq_excl)
  WITH (EXCLUDE ALL CONSTRAINTS);
contains:not available

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_exclude_constraints_option = true
