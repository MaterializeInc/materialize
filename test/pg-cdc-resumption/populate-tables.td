# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ postgres-execute connection=postgres://postgres:postgres@postgres
DROP TABLE IF EXISTS ten;
CREATE TABLE ten (f1 INTEGER);
ALTER TABLE ten REPLICA IDENTITY FULL;
INSERT INTO ten VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10);

DROP TABLE IF EXISTS t0;
CREATE TABLE t0 (f1 INTEGER, f2 TEXT);
ALTER TABLE t0 REPLICA IDENTITY FULL;
ALTER PUBLICATION mz_source ADD TABLE t0;

DROP TABLE IF EXISTS t1;
CREATE TABLE t1 (f1 INTEGER, f2 TEXT);
ALTER TABLE t1 REPLICA IDENTITY FULL;
ALTER PUBLICATION mz_source ADD TABLE t1;

INSERT INTO t1 SELECT a1.f1 + a6.f1 , E'abc\nxyz' FROM ten AS a1, ten AS a2, ten AS a3, ten AS a4, ten AS a5, ten AS a6;

DROP TABLE IF EXISTS t2;
CREATE TABLE t2 (f1 INTEGER, f2 TEXT);
ALTER TABLE t2 REPLICA IDENTITY FULL;
ALTER PUBLICATION mz_source ADD TABLE t2;

INSERT INTO t2 SELECT a1.f1 + a6.f1 , E'abc\nxyz' FROM ten AS a1, ten AS a2, ten AS a3, ten AS a4, ten AS a5, ten AS a6;

DROP TABLE IF EXISTS to_be_altered;
CREATE TABLE to_be_altered (f1 INTEGER);
ALTER TABLE to_be_altered REPLICA IDENTITY FULL;

INSERT INTO to_be_altered VALUES (1);
ALTER PUBLICATION mz_source_alter ADD TABLE to_be_altered;
