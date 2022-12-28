# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ postgres-execute connection=postgres://postgres:postgres@postgres
CREATE USER debezium WITH SUPERUSER PASSWORD 'debezium';
GRANT ALL PRIVILEGES ON DATABASE "postgres" TO debezium;
GRANT ALL PRIVILEGES ON SCHEMA "public" TO debezium;

CREATE PUBLICATION mz_source;

$ postgres-execute connection=postgres://postgres:postgres@postgres
DROP TABLE IF EXISTS one;
CREATE TABLE one (f1 INTEGER);
ALTER TABLE one REPLICA IDENTITY FULL;
INSERT INTO one VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10);
ALTER PUBLICATION mz_source ADD TABLE one;

DROP TABLE IF EXISTS two;
CREATE TABLE two (f1 INTEGER);
ALTER TABLE two REPLICA IDENTITY FULL;
INSERT INTO two VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10);
ALTER PUBLICATION mz_source ADD TABLE two;
