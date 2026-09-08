# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ mysql-connect name=mysql url=mysql://root@${arg.mysql-host} password=${arg.mysql-root-password}

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

# Tables with a single-column primary key take the parallel PK-range snapshot
# path when the source runs on a multi-worker cluster (see create-source.td),
# one integer PK and one text PK to cover both boundary-literal paths. The f2
# values are derived from the sequence so verify-data.td can assert exact
# checksums, catching duplicated or dropped PK ranges that COUNT alone masks.

DROP TABLE IF EXISTS t1pk;
CREATE TABLE t1pk (pk BIGINT PRIMARY KEY, f2 BIGINT);
SET @i := 0;
INSERT INTO t1pk SELECT @i := @i + 1, @i * 3 FROM ten AS a1, ten AS a2, ten AS a3, ten AS a4, ten AS a5, ten AS a6 LIMIT 500000;

DROP TABLE IF EXISTS t2pk;
CREATE TABLE t2pk (pk VARCHAR(32) PRIMARY KEY, f2 BIGINT);
SET @j := 0;
INSERT INTO t2pk SELECT CONCAT('key-', LPAD(CONV(@j := @j + 1, 10, 36), 8, '0')), @j FROM ten AS a1, ten AS a2, ten AS a3, ten AS a4, ten AS a5, ten AS a6 LIMIT 250000;

DROP TABLE IF EXISTS alter_fail_drop_col;
CREATE TABLE alter_fail_drop_col (f1 INTEGER, f2 INTEGER);

INSERT INTO alter_fail_drop_col VALUES (1, 1);

DROP TABLE IF EXISTS alter_fail_drop_constraint;
CREATE TABLE alter_fail_drop_constraint (f1 INTEGER NOT NULL);

INSERT INTO alter_fail_drop_constraint VALUES (1);
