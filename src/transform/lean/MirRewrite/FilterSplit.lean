-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file.

-- Hand-authored proof for the filter_split rewrite (src/transform/src/eqsat/
-- filter_split.rs). The rule is registered in Rust, not in relational.rewrite,
-- so gen-lean never sees it and this theorem is maintained by hand.
--
-- filter_split is the inverse of merge_filters: peeling one predicate outward,
-- Filter[p AND q](r) = Filter[p](Filter[q](r)). The bag model uses total
-- Row -> Bool predicates, so predicate evaluation order is irrelevant here. The
-- execution-level error-ordering equivalence (a q-error on a p-false row) is
-- justified separately by Materialize's error-as-data envelope, the same
-- justification the filter-movement rules rely on. The bag model does not model
-- errors, so it is silent on that point by construction, not by hand-waving.
import MirRewrite.Semantics

namespace MirRewrite

theorem rule_filter_split :
    ∀ (p : Row → Bool) (q : Row → Bool) (r : Bag), filterB (predAnd q p) r = filterB p (filterB q r) := by
    intro p q r; funext x; simp only [filterB, unionB, negateB, thresholdB, predAnd, emptyBag]; cases p x <;> cases q x <;> simp_all <;> try omega

end MirRewrite
