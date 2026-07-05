-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file.

-- Hand-authored statement for the map_split rewrite (src/transform/src/eqsat/
-- map_split.rs). The rule is registered in Rust, not in relational.rewrite, so
-- gen-lean never sees it. map_split is the reverse of fuse_maps, peeling the
-- first scalar: mapB (catRows s1 s2) r = mapB s2 (mapB s1 r). Like fuse_maps
-- (and fuse_projects, push_filter_through_map), Map acts on row/column
-- structure, which the bag model (Row -> Int multiplicity) does not represent,
-- so this is a sorry, the same established modeling boundary as its forward
-- sibling rule_fuse_maps, not a new unverified claim.
import MirRewrite.Semantics

namespace MirRewrite

theorem rule_map_split :
    ∀ (s1 : Row → Row) (s2 : Row → Row) (r : Bag), mapB (catRows s1 s2) r = mapB s2 (mapB s1 r) := by
    -- not modeled at the bag level (acts on row/column structure); see rule_fuse_maps
    sorry

end MirRewrite
