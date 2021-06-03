# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER PUBLICATION mz_source ADD TABLE t3;
INSERT INTO t3 SELECT a1.f1 + a6.f1 , E'abc\nxyz' FROM ten AS a1, ten AS a2, ten AS a3, ten AS a4, ten AS a5, ten AS a6;

> CREATE MATERIALIZED VIEWS FROM SOURCE mz_source (t3);

$ set-sql-timeout duration=120s

> SELECT COUNT(*) = 2000000 FROM t3
true
