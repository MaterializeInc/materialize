-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file.

-- Root of the `MirRewrite` Lean library. Importing every module here makes it
-- the `lean_lib` default target's transitive closure, so `lake build` checks
-- the whole project: the hand-written denotational semantics and the
-- DSL-generated rewrite-soundness theorems. Without this aggregator the
-- generated theorems live in no build target and are never checked in
-- aggregate, only in isolation.
import MirRewrite.Semantics
import MirRewrite.Generated
import MirRewrite.FilterSplit
import MirRewrite.MapSplit
