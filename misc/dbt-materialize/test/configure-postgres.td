# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# IMPORTANT: The Postgres server has a custom pg_hba.conf that only
# accepts connections from specific users. You will have to update
# pg_hba.conf if you modify the existing user names or add new ones.

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER USER postgres WITH replication;

DROP SCHEMA IF EXISTS public CASCADE;
CREATE SCHEMA public;

DROP SCHEMA IF EXISTS jaffle CASCADE;
CREATE SCHEMA jaffle;

CREATE TABLE pg_t1 (pk INTEGER PRIMARY KEY, f2 TEXT);
ALTER TABLE pg_t1 REPLICA IDENTITY FULL;

INSERT INTO pg_t1 VALUES (1, 'one');
INSERT INTO pg_t1 VALUES (2, 'two');
INSERT INTO pg_t1 VALUES (3, 'three');

CREATE TABLE jaffle.pg_t2 (pk INTEGER PRIMARY KEY, f2 TEXT);
ALTER TABLE jaffle.pg_t2 REPLICA IDENTITY FULL;

INSERT INTO jaffle.pg_t2 VALUES (1, 'one');
INSERT INTO jaffle.pg_t2 VALUES (2, 'two');
INSERT INTO jaffle.pg_t2 VALUES (3, 'three');

DROP PUBLICATION IF EXISTS mz_source;
CREATE PUBLICATION mz_source FOR ALL TABLES;

> DROP SCHEMA IF EXISTS jaffle

> CREATE SCHEMA jaffle
