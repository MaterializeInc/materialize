-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file.

import Lake
open Lake DSL

package «mirRewrite» where
  -- Mathlib-free: only Lean 4 core is required, so `lake build` is cheap.

@[default_target]
lean_lib «MirRewrite» where
  -- Includes Semantics.lean (hand-written) and Generated.lean (from the DSL).
