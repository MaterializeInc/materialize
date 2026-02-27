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
  - c1: bigint
----
Source defined as t0

# 3-arm chain on #0: should produce a CaseLiteral.

apply pipeline=case_literal
Map (case when (#0 = 0) then 10 else case when (#0 = 1) then 20 else case when (#0 = 2) then 30 else 40 end end end)
  Get t0
----
Map (case #0 when 0 then 10 when 1 then 20 when 2 then 30 else 40 end)
  Get t0

# 2-arm chain (minimum): should produce a CaseLiteral.

apply pipeline=case_literal
Map (case when (#0 = 1) then 10 else case when (#0 = 2) then 20 else 0 end end)
  Get t0
----
Map (case #0 when 1 then 10 when 2 then 20 else 0 end)
  Get t0

# Single arm: should NOT produce a CaseLiteral.

apply pipeline=case_literal
Map (case when (#0 = 1) then 10 else 0 end)
  Get t0
----
Map (case when (#0 = 1) then 10 else 0 end)
  Get t0

# Different columns break the chain: no CaseLiteral.

apply pipeline=case_literal
Map (case when (#0 = 1) then 10 else case when (#1 = 2) then 20 else 0 end end)
  Get t0
----
Map (case when (#0 = 1) then 10 else case when (#1 = 2) then 20 else 0 end end)
  Get t0

# Partial extraction: first 2 arms match #0, then #1 interrupts.
# Should produce a CaseLiteral for the first 2 arms with
# the remaining If as the fallback.

apply pipeline=case_literal
Map (case when (#0 = 1) then 10 else case when (#0 = 2) then 20 else case when (#1 = 3) then 30 else 0 end end end)
  Get t0
----
Map (case #0 when 1 then 10 when 2 then 20 else case when (#1 = 3) then 30 else 0 end end)
  Get t0

# Interrupting arm in the middle: first 2 on #0, then #1, then 2 on #0.
# Should produce two CaseLiterals with the #1 comparison in between.

apply pipeline=case_literal
Map (case when (#0 = 1) then 100 else case when (#0 = 2) then 200 else case when (#1 = 99) then 999 else case when (#0 = 3) then 300 else case when (#0 = 4) then 400 else 0 end end end end end)
  Get t0
----
Map (case #0 when 1 then 100 when 2 then 200 else case when (#1 = 99) then 999 else case #0 when 3 then 300 when 4 then 400 else 0 end end end)
  Get t0

# Nested CaseLiteral in a result arm: the inner chain (in the then branch)
# should also be converted.

apply pipeline=case_literal
Map (case when (#0 = 1) then case when (#1 = 10) then 100 else case when (#1 = 20) then 200 else 0 end end else case when (#0 = 2) then 50 else case when (#0 = 3) then 60 else 0 end end end)
  Get t0
----
Map (case #0 when 1 then case #1 when 10 then 100 when 20 then 200 else 0 end when 2 then 50 when 3 then 60 else 0 end)
  Get t0

# CaseLiteral in a filter predicate.

apply pipeline=case_literal
Filter (case when (#0 = 1) then true else case when (#0 = 2) then true else false end end)
  Get t0
----
Filter case #0 when 1 then true when 2 then true else false end
  Get t0

# Duplicate literal: second occurrence should be ignored (first wins).

apply pipeline=case_literal
Map (case when (#0 = 1) then 10 else case when (#0 = 1) then 99 else case when (#0 = 2) then 20 else 0 end end end)
  Get t0
----
Map (case when (#0 = 1) then 10 else case #0 when 1 then 99 when 2 then 20 else 0 end end)
  Get t0
