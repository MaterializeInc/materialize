# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Source definitions
# ------------------

define
DefSource name=t0
  - c0: bigint
  - c1: bigint?
----
Source defined as t0

# then branch

apply pipeline=coalesce_case
Map (coalesce(case when (#0 = 0) then null::bigint else 2 end, 1))
  Get t0
----
Map (case when (#0 = 0) then coalesce(1) else coalesce(2, 1) end)
  Get t0

# else branch

apply pipeline=coalesce_case
Map (coalesce(case when (#0 = 0) then 1 else null::bigint end, 2))
  Get t0
----
Map (case when (#0 = 0) then coalesce(1, 2) else coalesce(2) end)
  Get t0

# doesn't apply

apply pipeline=coalesce_case
Map (coalesce(case when (#0 = 0) then 1 else 2 end, 3))
  Get t0
----
Map (coalesce(case when (#0 = 0) then 1 else 2 end, 3))
  Get t0

# then branch, nullable else

apply pipeline=coalesce_case
Map (coalesce(case when (#0 = 0) then null::bigint else #1 end, 1))
  Get t0
----
Map (case when (#0 = 0) then coalesce(1) else coalesce(#1, 1) end)
  Get t0

# else branch, nullable then

apply pipeline=coalesce_case
Map (coalesce(case when (#0 = 0) then #1 else null::bigint end, 2))
  Get t0
----
Map (case when (#0 = 0) then coalesce(#1, 2) else coalesce(2) end)
  Get t0
