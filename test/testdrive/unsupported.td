# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-sql-timeout duration=125ms

! UPDATE foo SET x = 1
UPDATE statements not yet supported

! DELETE FROM foo
DELETE statements not yet supported

# DROP MATERIALIZED <anything> is not allowed
! DROP MATERIALIZED VIEW foo
DROP MATERIALIZED VIEW is not allowed, use DROP VIEW

! DROP MATERIALIZED SOURCE foo
DROP MATERIALIZED SOURCE is not allowed, use DROP SOURCE

! DROP MATERIALIZED TABLE foo
DROP MATERIALIZED TABLE is not allowed, use DROP TABLE
