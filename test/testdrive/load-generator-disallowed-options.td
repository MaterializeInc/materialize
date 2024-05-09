# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_create_source_denylist_with_options = true

$ set-arg-default default-replica-size=1
$ set-arg-default single-replica-cluster=quickstart

! CREATE SOURCE auction_house
  IN CLUSTER ${arg.single-replica-cluster}
  FROM LOAD GENERATOR AUCTION
  KEY FORMAT TEXT
  VALUE FORMAT TEXT
  INCLUDE KEY AS renamed_id
  FOR ALL TABLES;
contains:INCLUDE metadata only supported with `KEY VALUE` load generators

! CREATE SOURCE auction_house
  IN CLUSTER ${arg.single-replica-cluster}
  FROM LOAD GENERATOR AUCTION
  ENVELOPE DEBEZIUM
  FOR ALL TABLES;
contains:ENVELOPE other than NONE (except for KEY VALUE) with load generator source is not supported

! CREATE SOURCE auction_house
  IN CLUSTER ${arg.single-replica-cluster}
  FROM LOAD GENERATOR AUCTION
  FORMAT TEXT
  FOR ALL TABLES;
contains:FORMAT with load generator source is not supported

! CREATE SOURCE auction_house
  IN CLUSTER ${arg.single-replica-cluster}
  FROM LOAD GENERATOR AUCTION
  INCLUDE HEADERS
  FOR ALL TABLES;
contains:INCLUDE metadata only supported with `KEY VALUE` load generators
