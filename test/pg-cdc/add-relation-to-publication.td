# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ postgres-execute connection=postgres://postgres:postgres@postgres
CREATE TABLE pk_table (pk INTEGER PRIMARY KEY, f2 TEXT);
INSERT INTO pk_table VALUES (1, 'one');
ALTER TABLE pk_table REPLICA IDENTITY FULL;
INSERT INTO pk_table VALUES (2, 'two');

DROP PUBLICATION IF EXISTS mz_source;
CREATE PUBLICATION mz_source FOR TABLE pk_table;

CREATE TABLE nonpk_table (f1 INTEGER, f2 INTEGER);
INSERT INTO nonpk_table VALUES (1, 1), (1, 1);
ALTER TABLE nonpk_table REPLICA IDENTITY FULL;
INSERT INTO nonpk_table VALUES (2, 2), (2, 2);

> CREATE MATERIALIZED SOURCE "mz_source"
  FROM POSTGRES HOST 'host=postgres port=5432 user=postgres password=postgres sslmode=require dbname=postgres'
  PUBLICATION 'mz_source';

> SELECT COUNT(*) FROM mz_source
2

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER PUBLICATION mz_source ADD TABLE nonpk_table;

# Added relations will not be synced until Postgres emits a data event!
> SELECT COUNT(*) FROM mz_source
2

$ postgres-execute connection=postgres://postgres:postgres@postgres
INSERT INTO nonpk_table VALUES (3, 3);

$ set-sql-timeout duration=100s

> SELECT COUNT(*) FROM mz_source
7

> CREATE MATERIALIZED VIEWS FROM SOURCE mz_source (nonpk_table)

> SELECT * FROM nonpk_table
1 1
1 1
2 2
2 2
3 3
