# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ http-request method=POST url=http://toxiproxy:8474/proxies content-type=application/json
{
  "name": "postgres",
  "listen": "0.0.0.0:5432",
  "upstream": "postgres:5432",
  "enabled": true
}

> CREATE SECRET pgpass AS 'postgres'

> CREATE CONNECTION pg TO POSTGRES (
    HOST toxiproxy,
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

CREATE TABLE t (a int);
INSERT INTO t VALUES (1);
ALTER TABLE t REPLICA IDENTITY FULL;

> BEGIN
> CREATE SOURCE pg_source
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source');
> CREATE TABLE t FROM SOURCE pg_source (REFERENCE "t");
> COMMIT

> SELECT * FROM t;
1

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE t DROP COLUMN a;
ALTER TABLE t ADD COLUMN a int;
INSERT INTO t VALUES (2);

> SELECT name, status FROM mz_internal.mz_source_statuses;
pg_source           running
t                   ceased

> SELECT
        status = 'ceased'
            AND
        error ILIKE '%incompatible schema change%'
    FROM mz_internal.mz_source_statuses
    WHERE name = 't';
true

! SELECT * FROM t;
contains: incompatible schema change
