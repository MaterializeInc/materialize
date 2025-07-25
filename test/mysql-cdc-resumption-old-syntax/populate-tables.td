# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ mysql-connect name=mysql url=mysql://root@${arg.mysql-host}:${arg.mysql-port} password=${arg.mysql-root-password}

$ mysql-execute name=mysql
USE public;

DROP TABLE IF EXISTS ten;
CREATE TABLE ten (f1 INTEGER);
INSERT INTO ten VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10);

DROP TABLE IF EXISTS t0;
CREATE TABLE t0 (f1 INTEGER, f2 TEXT);

DROP TABLE IF EXISTS t1;
CREATE TABLE t1 (f1 INTEGER, f2 TEXT);

INSERT INTO t1 SELECT a1.f1 + a6.f1 , 'abc\nxyz' FROM ten AS a1, ten AS a2, ten AS a3, ten AS a4, ten AS a5, ten AS a6;

DROP TABLE IF EXISTS t2;
CREATE TABLE t2 (f1 INTEGER, f2 TEXT);

INSERT INTO t2 SELECT a1.f1 + a6.f1 , 'abc\nxyz' FROM ten AS a1, ten AS a2, ten AS a3, ten AS a4, ten AS a5, ten AS a6;

DROP TABLE IF EXISTS alter_fail_drop_col;
CREATE TABLE alter_fail_drop_col (f1 INTEGER, f2 INTEGER);

INSERT INTO alter_fail_drop_col VALUES (1, 1);

DROP TABLE IF EXISTS alter_fail_drop_constraint;
CREATE TABLE alter_fail_drop_constraint (f1 INTEGER NOT NULL);

INSERT INTO alter_fail_drop_constraint VALUES (1);
