# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Regression test for SQL-530: a graceful cluster reconfiguration (WAIT UNTIL
# READY) of a cluster hosting a Postgres source must resize and cut over
# instead of waiting forever for the source to hydrate on the new replica.
#
# A Postgres source prefers a single replica, so the controller keeps its
# ingestion on the replica it already runs on and never schedules it onto the
# new replica until cut-over drops the old one. Readiness must therefore not
# wait for the source to hydrate on the target, but must still wait for the
# target's processes to come online before cutting over.
#
# Both reconfiguration paths are exercised below, the legacy foreground path
# and the controller-owned path. The path-selection flags are pinned
# explicitly so the test does not depend on the harness defaults.

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_zero_downtime_cluster_reconfiguration = true

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

CREATE TABLE t1 (f1 INTEGER PRIMARY KEY);
ALTER TABLE t1 REPLICA IDENTITY FULL;
INSERT INTO t1 VALUES (1), (2), (3);

CREATE PUBLICATION mz_source FOR TABLE t1;

# Host the source on its own single-replica cluster so the reconfiguration has
# exactly one source replica to replace.
> CREATE CLUSTER source_reconfig (SIZE 'scale=1,workers=1', REPLICATION FACTOR 1)

> CREATE SOURCE mz_source
  IN CLUSTER source_reconfig
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source')

> CREATE TABLE t1 FROM SOURCE mz_source (REFERENCE t1)

> SELECT * FROM t1
1
2
3

# ----- Legacy foreground path -----

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_cluster_controller = false
ALTER SYSTEM SET enable_background_alter_cluster = false

# The ALTER blocks until readiness passes. Before the fix it would block until
# the deadline rolls the resize back, failing the test at the statement
# timeout. The raised timeout only gives the success path headroom on slow CI.
$ set-sql-timeout duration=120s

> ALTER CLUSTER source_reconfig SET (SIZE 'scale=1,workers=2') WITH (WAIT UNTIL READY (TIMEOUT '300s', ON TIMEOUT 'ROLLBACK'))

$ set-sql-timeout duration=default

# The realized size advanced to the target: the cut-over happened.
> SELECT size FROM mz_clusters WHERE name = 'source_reconfig'
"scale=1,workers=2"

# The source still serves its data after cut-over.
> SELECT * FROM t1
1
2
3

# And it keeps ingesting on the promoted replica.
$ postgres-execute connection=postgres://postgres:postgres@postgres
INSERT INTO t1 VALUES (4), (5);

> SELECT * FROM t1
1
2
3
4
5

# ----- Controller-owned path -----

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_cluster_controller = true
ALTER SYSTEM SET enable_background_alter_cluster = true

# With the background flag on the ALTER returns immediately and the controller
# drives readiness and cut-over. Before the fix the resize would roll back at
# its deadline and the realized size below would never advance.
> ALTER CLUSTER source_reconfig SET (SIZE 'scale=1,workers=1') WITH (WAIT UNTIL READY (TIMEOUT '300s', ON TIMEOUT 'ROLLBACK'))

# The raised timeout covers replica boot plus the controller's tick cadence.
$ set-sql-timeout duration=120s

> SELECT size FROM mz_clusters WHERE name = 'source_reconfig'
"scale=1,workers=1"

$ set-sql-timeout duration=default

# The source keeps serving and ingesting after the controller-driven cut-over.
$ postgres-execute connection=postgres://postgres:postgres@postgres
INSERT INTO t1 VALUES (6);

> SELECT * FROM t1
1
2
3
4
5
6

> DROP SOURCE mz_source CASCADE
> DROP CLUSTER source_reconfig
> DROP CONNECTION pg
> DROP SECRET pgpass
