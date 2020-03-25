# Copyright Materialize, Inc. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

> SELECT jsonb_agg(1)
jsonb_agg
-----------
[1.0]

! SELECT jsonb_agg(1, 2)
jsonb_agg function only takes one argument

# todo@jldlaughlin: Fix test when #2414 is implemented
! SELECT jsonb_agg((1, 2))
Parse error:
