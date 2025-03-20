# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET unsafe_enable_unorchestrated_cluster_replicas = true

$ set count=10000

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER USER postgres WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
DROP PUBLICATION IF EXISTS mz_source;

CREATE SCHEMA public;

CREATE TABLE t1 (pk SERIAL PRIMARY KEY, f2 BIGINT);
ALTER TABLE t1 REPLICA IDENTITY FULL;

INSERT INTO t1 (f2) SELECT x FROM generate_series(1, ${count}) as x;

CREATE PUBLICATION mz_source FOR ALL TABLES;

# Create a cluster with no replicas so that we have time to submit queries at the minimum frontier.
> CREATE CLUSTER storage REPLICAS ()

> CREATE SECRET pgpass AS 'postgres'

> CREATE CONNECTION pg TO POSTGRES (
    HOST postgres,
    DATABASE postgres,
    USER postgres,
    PASSWORD SECRET pgpass
  )
> CREATE SOURCE mz_source
  IN CLUSTER storage
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source')
  WITH (RETAIN HISTORY = FOR '365000 days');

> CREATE TABLE t1 FROM SOURCE mz_source (REFERENCE t1);

# Grab a cursor at timestamp 0
> BEGIN

> DECLARE c CURSOR FOR SELECT 1, COUNT(*) FROM t1 AS OF 0

# Start ingestion by adding a replica to the cluster. We must do this from a
# different connection to not disturbe the transaction we're in.
$ postgres-execute connection=postgres://materialize:materialize@${testdrive.materialize-sql-addr}
CREATE CLUSTER REPLICA storage.r1 SIZE = '1';

# Verify that at timestamp 0 there is only one record whose value is the final value
> FETCH 1 c WITH (timeout = '1d');
1 ${count}

> COMMIT
