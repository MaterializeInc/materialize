# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Without EXTENDED, SHOW TYPES shouldn't have anything to return.

! SELECT pg_get_constraintdef(885)
pg_get_constraintdef is not yet implemented is undefined

! SELECT pg_get_constraintdef(885, true)
pg_get_constraintdef is not yet implemented is undefined

> SELECT pg_get_constraintdef(pg_constraint.oid) FROM pg_constraint
pg_get_constraintdef
-------------------

> SELECT pg_get_constraintdef(pg_constraint.oid, true) FROM pg_constraint
pg_get_constraintdef
-------------------
