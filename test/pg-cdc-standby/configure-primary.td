# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Seed the primary. The standby is cloned from it afterwards, so these objects
# (and the postgres role's REPLICATION attribute, which it has by default)
# arrive on the standby via the physical base backup.

$ postgres-execute connection=postgres://postgres:postgres@pg-primary
DROP TABLE IF EXISTS t;
CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);
ALTER TABLE t REPLICA IDENTITY FULL;
INSERT INTO t SELECT g, 'v' || g FROM generate_series(1, 1000) AS g;

DROP TABLE IF EXISTS small;
CREATE TABLE small (id INTEGER PRIMARY KEY, v TEXT);
ALTER TABLE small REPLICA IDENTITY FULL;
INSERT INTO small VALUES (1, 'a'), (2, 'b'), (3, 'c');

DROP PUBLICATION IF EXISTS mz_source;
CREATE PUBLICATION mz_source FOR ALL TABLES;
ANALYZE;
