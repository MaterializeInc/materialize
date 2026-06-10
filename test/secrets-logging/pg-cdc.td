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
CREATE SCHEMA public;

DROP PUBLICATION IF EXISTS mz_source;
CREATE PUBLICATION mz_source FOR ALL TABLES;

CREATE TABLE t (f1 TEXT);
INSERT INTO t VALUES ('ghp_9fK8sL3x7TqR1vEzYm2pDaN4WjXbQzUtV0aN');
ALTER TABLE t REPLICA IDENTITY FULL;

> BEGIN
> CREATE SOURCE pg_source
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source');

> CREATE TABLE t FROM SOURCE pg_source (REFERENCE t);
> COMMIT

> SELECT * FROM t
ghp_9fK8sL3x7TqR1vEzYm2pDaN4WjXbQzUtV0aN
