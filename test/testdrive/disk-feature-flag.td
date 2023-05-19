# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test that with `enable_create_source_denylist_with_options` off, if
# `enable_upsert_source_disk` is on, we can use `WITH(DISK)`.
# Note that `WITH(DISK)` is not tested in `test/testdrive`, so we use a source
# type that will show that we allow the with option overall, but fail later on in
# `plan_create_source`.
$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_create_source_denylist_with_options = false
ALTER SYSTEM SET enable_upsert_source_disk = true

! CREATE SOURCE lg FROM LOAD GENERATOR COUNTER
  WITH (DISK)
contains:ON DISK used with non-UPSERT/DEBEZIUM ENVELOPE not yet supported

$ postgres-execute connection=postgres://mz_system:materialize@${testdrive.materialize-internal-sql-addr}
ALTER SYSTEM SET enable_upsert_source_disk = false

! CREATE SOURCE lg FROM LOAD GENERATOR COUNTER
  WITH (DISK)
contains:CREATE SOURCE...WITH (DISK..) is not supported
