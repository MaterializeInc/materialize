# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# This test "reverts" a schema change that causes a PG subsource to
# enter a failed state.
$ postgres-execute connection=postgres://postgres:postgres@postgres
DELETE FROM alter_fail_drop_constraint WHERE f1 IS NULL;
ALTER TABLE alter_fail_drop_constraint ALTER COLUMN f1 SET NOT NULL;
INSERT INTO alter_fail_drop_constraint VALUES (1);
