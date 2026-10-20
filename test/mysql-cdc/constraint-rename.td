# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Reproduction for SS-531: renaming a UNIQUE constraint upstream leaves the
# key Materialize planned with unchanged, but the table stalls with a
# permanent error.
#
# MySQL names every primary key `PRIMARY` and does not allow renaming it, so
# only UNIQUE constraints (which are indexes) can be renamed, with
# `ALTER TABLE ... RENAME INDEX`.
#
# This file asserts the current behavior. Each stall marked SS-531 is the
# behavior the issue wants changed: a rename should be a non-event.

$ set-sql-timeout duration=60s

> CREATE SECRET mysqlpass AS '${arg.mysql-root-password}'
> CREATE CONNECTION mysql_conn TO MYSQL (
    HOST mysql,
    USER root,
    PASSWORD SECRET mysqlpass
  )

$ mysql-connect name=mysql url=mysql://root@mysql password=${arg.mysql-root-password}

$ mysql-execute name=mysql
DROP DATABASE IF EXISTS public;
CREATE DATABASE public;
USE public;

CREATE TABLE rename_uq (id INT NOT NULL PRIMARY KEY, wallet VARCHAR(64) NOT NULL, CONSTRAINT rename_uq_wallet UNIQUE (wallet));
INSERT INTO rename_uq VALUES (1, 'a');

CREATE TABLE rename_composite (a INT NOT NULL, b INT NOT NULL, val TEXT, CONSTRAINT rename_composite_ab UNIQUE (a, b));
INSERT INTO rename_composite VALUES (1, 1, 'a');

> CREATE SOURCE mz_source FROM MYSQL CONNECTION mysql_conn;
> CREATE TABLE rename_uq FROM SOURCE mz_source (REFERENCE public.rename_uq);
> CREATE TABLE rename_composite FROM SOURCE mz_source (REFERENCE public.rename_composite);

> SELECT * FROM rename_uq;
1 a

> SELECT * FROM rename_composite;
1 1 a

#
# Rename a UNIQUE constraint. The index keeps its columns.
#

$ mysql-execute name=mysql
ALTER TABLE rename_uq RENAME INDEX rename_uq_wallet TO rename_uq_wallet_v2;
INSERT INTO rename_uq VALUES (2, 'b');

# SS-531: the key is unchanged, but keys are matched by name, so the rename
# is reported as a drop and the table stalls permanently.
! SELECT * FROM rename_uq;
contains:incompatible schema change on public.rename_uq: UNIQUE constraint "rename_uq_wallet" (wallet) was dropped upstream

#
# Rename a composite UNIQUE constraint.
#

$ mysql-execute name=mysql
ALTER TABLE rename_composite RENAME INDEX rename_composite_ab TO rename_composite_ab_v2;
INSERT INTO rename_composite VALUES (2, 2, 'b');

# SS-531: same for a composite key.
! SELECT * FROM rename_composite;
contains:incompatible schema change on public.rename_composite: UNIQUE constraint "rename_composite_ab" (a, b) was dropped upstream

> DROP SOURCE mz_source CASCADE;
