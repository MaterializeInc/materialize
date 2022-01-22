# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-sql-timeout duration=125ms

# DROP MATERIALIZED <anything> is not allowed
! DROP MATERIALIZED VIEW foo
contains:DROP MATERIALIZED VIEW is not allowed, use DROP VIEW

! DROP MATERIALIZED SOURCE foo
contains:DROP MATERIALIZED SOURCE is not allowed, use DROP SOURCE

! DROP MATERIALIZED TABLE foo
contains:DROP MATERIALIZED TABLE is not allowed, use DROP TABLE
