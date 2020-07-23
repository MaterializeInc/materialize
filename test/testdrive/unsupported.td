# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set-sql-timeout duration=125ms

! INSERT INTO foo VALUES (1)
INSERT statements are not supported

! UPDATE foo SET x = 1
UPDATE statements are not supported

! DELETE FROM foo
DELETE statements are not supported
