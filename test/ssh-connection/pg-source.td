# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test creating a Postgres source using SSH

> CREATE SECRET pgpass AS 'postgres'

! CREATE CONNECTION pg TO POSTGRES (
    HOST postgres,
    HOST postgres2,
    DATABASE postgres,
    USER postgres,
    PASSWORD SECRET pgpass,
    SSH TUNNEL thancred
  );
contains: HOST specified more than once

> CREATE CONNECTION pg TO POSTGRES (
    HOST postgres,
    DATABASE postgres,
    USER postgres,
    PASSWORD SECRET pgpass,
    SSH TUNNEL thancred
  );

# The route owner can use the fixed connection without authority to redirect
# its password, including before there is an active source.
$ postgres-execute connection=mz_system
ALTER SYSTEM SET enable_rbac_checks = true;
ALTER SECRET pgpass OWNER TO mz_system;
CREATE ROLE route_validator_${testdrive.seed} LOGIN PASSWORD 'validatorpass';
GRANT USAGE ON SCHEMA materialize.public TO route_validator_${testdrive.seed};
GRANT USAGE ON CONNECTION pg TO route_validator_${testdrive.seed};

! ALTER CONNECTION thancred SET (HOST 'redirected.invalid') WITH (VALIDATE = false)
contains:permission denied for SECRET "materialize.public.pgpass"

! ALTER CONNECTION thancred SET (HOST 'redirected.invalid') WITH (VALIDATE = true)
contains:permission denied for SECRET "materialize.public.pgpass"

$ postgres-connect name=route_validator url=postgres://route_validator_${testdrive.seed}:validatorpass@${testdrive.materialize-sql-addr}

$ postgres-execute connection=route_validator
VALIDATE CONNECTION pg;

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER USER postgres WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
DROP PUBLICATION IF EXISTS mz_source;
CREATE SCHEMA public;

CREATE TABLE t1 (f1 INTEGER);
ALTER TABLE t1 REPLICA IDENTITY FULL;
INSERT INTO t1 VALUES (1);

CREATE PUBLICATION mz_source FOR ALL TABLES;

> BEGIN
> CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg
  (PUBLICATION 'mz_source');

> CREATE TABLE t1 FROM SOURCE mz_source (REFERENCE t1);
> COMMIT

> SELECT COUNT(*) = 1 FROM t1;
true

> SELECT f1 FROM t1;
1

! ALTER CONNECTION thancred SET (HOST 'redirected.invalid') WITH (VALIDATE = false)
contains:permission denied for SECRET "materialize.public.pgpass"

$ postgres-execute connection=route_validator
VALIDATE CONNECTION pg;

$ postgres-execute connection=postgres://postgres:postgres@postgres
INSERT INTO t1 VALUES (1), (2);

> SELECT f1 FROM t1 ORDER BY f1 ASC;
1
1
2

# Granting secret access permits route administration with an active dependent.
$ postgres-execute connection=mz_system
GRANT USAGE ON SECRET pgpass TO materialize;

> ALTER CONNECTION thancred SET (HOST 'ssh-bastion-host') WITH (VALIDATE = true)

> SELECT f1 FROM t1 ORDER BY f1 ASC;
1
1
2

$ postgres-execute connection=mz_system
ALTER SECRET pgpass OWNER TO materialize;
