# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ postgres-execute connection=postgres://postgres:postgres@postgres
DROP SCHEMA IF EXISTS pgsink CASCADE;
DROP SCHEMA IF EXISTS materialize CASCADE;
CREATE SCHEMA pgsink;

> CREATE CLUSTER pgsink_cluster SIZE 'scale=1,workers=1', REPLICATION FACTOR 1
> CREATE TABLE pgsink_input (a int NOT NULL, b text)
> INSERT INTO pgsink_input VALUES (1, 'duplicate'), (2, 'duplicate')
> CREATE MATERIALIZED VIEW pgsink_values AS SELECT b FROM pgsink_input

> CREATE SINK pgsink_keyed IN CLUSTER pgsink_cluster FROM pgsink_input
  INTO POSTGRES CONNECTION pg (SCHEMA 'pgsink', TABLE 'keyed') KEY (a) NOT ENFORCED
> CREATE SINK pgsink_keyless IN CLUSTER pgsink_cluster FROM pgsink_values
  INTO POSTGRES CONNECTION pg (SCHEMA 'pgsink', TABLE 'keyless')

> SELECT count(*) FROM mz_internal.mz_frontiers
  WHERE object_id IN (SELECT id FROM mz_sinks WHERE name LIKE 'pgsink_%') AND write_frontier > 0
2

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE pgsink.keyed REPLICA IDENTITY FULL;

> CREATE SOURCE pgsink_source FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source')
> CREATE TABLE pgsink_keyed_readback FROM SOURCE pgsink_source (REFERENCE pgsink.keyed)
> CREATE TABLE pgsink_keyless_readback FROM SOURCE pgsink_source (REFERENCE pgsink.keyless)

> SELECT * FROM pgsink_keyed_readback
1 duplicate
2 duplicate
> SELECT * FROM pgsink_keyless_readback
duplicate
duplicate

> DELETE FROM pgsink_input WHERE a = 2
> UPDATE pgsink_input SET b = 'changed' WHERE a = 1
> INSERT INTO pgsink_input VALUES (3, 'three')

> SELECT * FROM pgsink_keyed_readback
1 changed
3 three
> SELECT * FROM pgsink_keyless_readback
changed
three
