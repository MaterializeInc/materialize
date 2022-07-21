# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ postgres-execute connection=postgres://postgres:postgres@postgres

DROP PUBLICATION IF EXISTS upgrade_pg_cdc_publication;
CREATE PUBLICATION upgrade_pg_cdc_publication FOR ALL TABLES;


DROP TABLE IF EXISTS upgrade_pg_cdc_table;
CREATE TABLE upgrade_pg_cdc_table (f1 INTEGER);
ALTER TABLE upgrade_pg_cdc_table REPLICA IDENTITY FULL;
INSERT INTO upgrade_pg_cdc_table VALUES (1),(2),(3),(4),(5);

> CREATE SECRET pgpass AS 'postgres';
> CREATE CONNECTION pgconn FOR POSTGRES
  HOST postgres,
  USER postgres,
  PASSWORD SECRET pgpass,
  DATABASE postgres;
> CREATE SOURCE upgrade_pg_cdc_source
  FROM POSTGRES
  CONNECTION pgconn
  PUBLICATION 'upgrade_pg_cdc_publication'

> CREATE VIEWS FROM SOURCE upgrade_pg_cdc_source;

> SELECT * FROM upgrade_pg_cdc_table;
1
2
3
4
5
