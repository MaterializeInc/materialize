# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ postgres-execute connection=postgres://postgres:postgres@postgres
ALTER TABLE alter_fail_add_col ADD COLUMN f2 INTEGER;
INSERT INTO alter_fail_add_col VALUES (1, 2);

ALTER TABLE alter_fail_drop_constraint ALTER COLUMN f1 DROP NOT NULL;
INSERT INTO alter_fail_drop_constraint VALUES (NULL);
