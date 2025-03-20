# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

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
DROP PUBLICATION IF EXISTS mz_source;

CREATE SCHEMA public;
CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');
CREATE TABLE person (name TEXT, current_mood mood);
INSERT INTO person VALUES ('Moe', 'happy');
ALTER TABLE person REPLICA IDENTITY FULL;

CREATE PUBLICATION mz_source FOR ALL TABLES;

! CREATE SOURCE mz_source FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source') FOR ALL TABLES;
contains:referenced tables use unsupported types
