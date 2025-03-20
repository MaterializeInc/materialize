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

> DROP SOURCE IF EXISTS mz_source CASCADE;
> DROP SECRET IF EXISTS pgpass CASCADE;
> DROP CONNECTION IF EXISTS pg CASCADE;

> CREATE CLUSTER storage REPLICAS (
    r1 (
      STORAGECTL ADDRESSES ['clusterd1:2100'],
      STORAGE ADDRESSES ['clusterd1:2103'],
      COMPUTECTL ADDRESSES ['clusterd1:2101'],
      COMPUTE ADDRESSES ['clusterd1:2102'],
      WORKERS 4
    )
  )

> CREATE SECRET pgpass AS 'postgres'
> CREATE CONNECTION pg TO POSTGRES (
    HOST postgres,
    DATABASE postgres,
    USER postgres,
    PASSWORD SECRET pgpass
  )

> CREATE SOURCE mz_source
  IN CLUSTER storage
  FROM POSTGRES
  CONNECTION pg
  (PUBLICATION 'mz_source');

> CREATE TABLE ten FROM SOURCE mz_source (REFERENCE ten);
> CREATE TABLE t1 FROM SOURCE mz_source (REFERENCE t1);
