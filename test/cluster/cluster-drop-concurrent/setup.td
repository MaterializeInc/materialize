# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_unorchestrated_cluster_replicas = true;

> DROP CLUSTER IF EXISTS drop CASCADE
> CREATE CLUSTER drop REPLICAS (replica1 (
  STORAGECTL ADDRESSES ['clusterd1:2100'],
  STORAGE ADDRESSES ['clusterd1:2103'],
  COMPUTECTL ADDRESSES ['clusterd1:2101'],
  COMPUTE ADDRESSES ['clusterd1:2102'],
  WORKERS 1))

> DROP SOURCE IF EXISTS counter CASCADE
> CREATE SOURCE counter IN CLUSTER drop FROM LOAD GENERATOR COUNTER (TICK INTERVAL '10s');
