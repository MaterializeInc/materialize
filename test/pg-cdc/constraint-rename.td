# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Reproduction for SS-531: renaming a PRIMARY KEY or UNIQUE constraint
# upstream leaves the key Materialize planned with unchanged, but the table
# stalls with a permanent error.
#
# This file asserts the current behavior. Each stall marked SS-531 is the
# behavior the issue wants changed: a rename should be a non-event.

$ set-sql-timeout duration=60s

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET pg_schema_validation_interval = '2s'

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

CREATE TABLE rename_uq (id int NOT NULL, wallet text NOT NULL, CONSTRAINT rename_uq_pk PRIMARY KEY (id), CONSTRAINT rename_uq_wallet UNIQUE (wallet));
INSERT INTO rename_uq VALUES (1, 'a');
ALTER TABLE rename_uq REPLICA IDENTITY FULL;

CREATE TABLE rename_pk (id int NOT NULL, val text, CONSTRAINT rename_pk_pk PRIMARY KEY (id));
INSERT INTO rename_pk VALUES (1, 'a');
ALTER TABLE rename_pk REPLICA IDENTITY FULL;

CREATE TABLE rename_composite (a int NOT NULL, b int NOT NULL, val text, CONSTRAINT rename_composite_ab UNIQUE (a, b));
INSERT INTO rename_composite VALUES (1, 1, 'a');
ALTER TABLE rename_composite REPLICA IDENTITY FULL;

> CREATE SOURCE mz_source FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source');
> CREATE TABLE rename_uq FROM SOURCE mz_source (REFERENCE rename_uq);
> CREATE TABLE rename_pk FROM SOURCE mz_source (REFERENCE rename_pk);
> CREATE TABLE rename_composite FROM SOURCE mz_source (REFERENCE rename_composite);

> SELECT * FROM rename_uq;
1 a

> SELECT * FROM rename_pk;
1 a

> SELECT * FROM rename_composite;
1 1 a

#
# Rename a UNIQUE constraint. The constraint keeps its oid and its columns.
#

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE rename_uq RENAME CONSTRAINT rename_uq_wallet TO rename_uq_wallet_v2;
INSERT INTO rename_uq VALUES (2, 'b');

# SS-531: the key is unchanged, but the name is part of the recorded key, so
# the rename is reported as an alteration and the table stalls permanently.
! SELECT * FROM rename_uq;
regex:incompatible schema change on public\.rename_uq \(oid \d+\): UNIQUE constraint "rename_uq_wallet" \(wallet\) was renamed or recreated upstream

#
# Rename a PRIMARY KEY constraint.
#

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE rename_pk RENAME CONSTRAINT rename_pk_pk TO rename_pk_pk_v2;
INSERT INTO rename_pk VALUES (2, 'b');

# SS-531: same for the primary key.
! SELECT * FROM rename_pk;
regex:incompatible schema change on public\.rename_pk \(oid \d+\): PRIMARY KEY constraint "rename_pk_pk" \(id\) was renamed or recreated upstream

#
# Rename a composite UNIQUE constraint.
#

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE rename_composite RENAME CONSTRAINT rename_composite_ab TO rename_composite_ab_v2;
INSERT INTO rename_composite VALUES (2, 2, 'b');

# SS-531: same for a composite key.
! SELECT * FROM rename_composite;
regex:incompatible schema change on public\.rename_composite \(oid \d+\): UNIQUE constraint "rename_composite_ab" \(a, b\) was renamed or recreated upstream

> DROP SOURCE mz_source CASCADE;
