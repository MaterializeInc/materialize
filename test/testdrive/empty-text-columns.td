# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER USER postgres WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
DROP PUBLICATION IF EXISTS mz_source;
CREATE SCHEMA public;
CREATE TABLE foo (id INT PRIMARY KEY, name TEXT);
ALTER TABLE foo REPLICA IDENTITY FULL;
INSERT INTO foo VALUES (1, 'hello');
CREATE PUBLICATION mz_source FOR ALL TABLES;
> CREATE SECRET pgpass AS 'postgres'
> CREATE CONNECTION pgconn TO POSTGRES (
    HOST postgres,
    DATABASE postgres,
    USER postgres,
    PASSWORD SECRET pgpass
  )
> CREATE SOURCE s FROM POSTGRES CONNECTION pgconn (PUBLICATION 'mz_source')

! CREATE TABLE foo FROM SOURCE s (REFERENCE = public.foo) WITH (TEXT COLUMNS = ());
contains: invalid TEXT COLUMNS option value: cannot be empty
