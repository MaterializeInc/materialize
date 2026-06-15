# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# All DML happens on the PRIMARY. It must reach Materialize via:
#   primary --(physical streaming replication)--> standby --(logical decoding)--> mz

$ postgres-execute connection=postgres://postgres:postgres@pg-primary
INSERT INTO t SELECT g, 'v' || g FROM generate_series(1001, 1500) AS g;
DELETE FROM t WHERE id <= 250;
UPDATE t SET v = 'updated' WHERE id = 1000;

INSERT INTO small VALUES (4, 'd');
DELETE FROM small WHERE id = 1;
UPDATE small SET v = 'B' WHERE id = 2;
