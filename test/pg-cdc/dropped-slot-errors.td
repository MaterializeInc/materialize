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

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER USER postgres WITH replication;
DROP SCHEMA IF EXISTS public CASCADE;
DROP PUBLICATION IF EXISTS mz_source;

CREATE SCHEMA public;

CREATE TABLE t1 (pk SERIAL PRIMARY KEY, f2 text);
ALTER TABLE t1 REPLICA IDENTITY FULL;

CREATE PUBLICATION mz_source FOR ALL TABLES;

> CREATE CLUSTER storage REPLICAS (r1 (SIZE 'scale=1,workers=1'))

> CREATE SECRET pgpass AS 'postgres'

> CREATE CONNECTION pg TO POSTGRES (
    HOST postgres,
    DATABASE postgres,
    USER postgres,
    PASSWORD SECRET pgpass
  )
> CREATE SOURCE mz_source
  IN CLUSTER storage
  FROM POSTGRES CONNECTION pg (PUBLICATION 'mz_source');

> CREATE TABLE t1 FROM SOURCE mz_source (REFERENCE t1);

# Wait for the initial snapshot to be ingested
> SELECT * FROM t1

# Stop ingestion by dropping the replica
> DROP CLUSTER REPLICA storage.r1;

# Now drop the replication slot
$ postgres-execute connection=postgres://postgres:postgres@postgres
SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots

# Resume the ingestion by adding a replica to the cluster.
> CREATE CLUSTER REPLICA storage.r1 SIZE = 'scale=1,workers=1';

> SELECT error ~~ 'postgres: slot overcompacted. Requested LSN % but only LSNs % are available%' FROM mz_internal.mz_source_statuses WHERE name = 't1';
true
