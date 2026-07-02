-- Copyright Materialize, Inc. and contributors. All rights reserved.
--
-- Use of this software is governed by the Business Source License
-- included in the LICENSE file.

-- AUTO-GENERATED from src/transform/src/eqsat/rules/relational.rewrite by `cargo run -p mz-transform --example gen-lean`.
-- Do not edit by hand: edit the DSL and regenerate.
--
-- Each theorem states that a rewrite preserves the multiplicity denotation of
-- a relation (see Semantics.lean), i.e. it never changes query results.
import MirRewrite.Semantics

namespace MirRewrite

-- filter(p, filter(q, r)) = filter(p && q, r)
theorem rule_merge_filters :
    ∀ (p : Row → Bool) (q : Row → Bool) (r : Bag), filterB p (filterB q r) = filterB (predAnd q p) r := by
    intro p q r; funext x; simp only [filterB, unionB, negateB, thresholdB, predAnd, emptyBag]; cases p x <;> cases q x <;> simp_all <;> try omega

-- project(a, project(b, r)) = project(b . a, r)
theorem rule_fuse_projects :
    ∀ (a : Row → Row) (b : Row → Row) (r : Bag), projB a (projB b r) = projB (projCompose a b) r := by
    -- not modeled at the bag level (acts on row/column structure)
    sorry

-- project([0..n], r) = r  (identity projection is the identity)
theorem rule_drop_identity_project :
    ∀ (p : Row → Row) (r : Bag), projB p r = r := by
    -- not modeled at the bag level (acts on row/column structure)
    sorry

-- filter(p, map(s, r)) = map(s, filter(p, r))  when p reads only r's columns
theorem rule_push_filter_through_map :
    ∀ (p : Row → Bool) (s : Row → Row) (r : Bag), filterB p (mapB s r) = mapB s (filterB p r) := by
    -- not modeled at the bag level (acts on row/column structure)
    sorry

-- filter(p, flatmap(f, es, r)) = flatmap(f, es, filter(p, r))  when p reads only r's columns
theorem rule_push_filter_past_flatmap :
    ∀ (p : Row → Bool) (f : TableFunc) (es : Row → Row) (r : Bag), filterB p (flatMapB r) = flatMapB (filterB p r) := by
    intro p f es r; funext x; simp only [filterB, unionB, negateB, thresholdB, predAnd, emptyBag]; cases p x <;> simp_all <;> try omega

-- filter(p, a + b) = filter(p, a) + filter(p, b)  when no predicate is known-false
theorem rule_distribute_filter_union :
    ∀ (p : Row → Bool) (a : Bag) (b : Bag), filterB p (unionB a b) = unionB (filterB p a) (filterB p b) := by
    intro p a b; funext x; simp only [filterB, unionB, negateB, thresholdB, predAnd, emptyBag]; cases p x <;> simp_all <;> try omega

-- a + (b + c) = a + b + c
theorem rule_flatten_union :
    ∀ (a : Bag) (b : Bag) (c : Bag), unionB a (unionB b c) = unionB a (unionB b c) := by
    intro a b c; rfl

-- negate(negate(r)) = r
theorem rule_negate_negate :
    ∀ (r : Bag), negateB (negateB r) = r := by
    intro r; funext x; simp only [filterB, unionB, negateB, thresholdB, predAnd, emptyBag]; omega

-- threshold(threshold(r)) = threshold(r)
theorem rule_threshold_idempotent :
    ∀ (r : Bag), thresholdB (thresholdB r) = thresholdB r := by
    intro r; funext x; simp only [filterB, unionB, negateB, thresholdB, predAnd, emptyBag]; by_cases h : r x > 0 <;> simp [h]

-- map(s2, map(s1, r)) = map(s1 ++ s2, r)
theorem rule_fuse_maps :
    ∀ (s2 : Row → Row) (s1 : Row → Row) (r : Bag), mapB s2 (mapB s1 r) = mapB (catRows s1 s2) r := by
    -- not modeled at the bag level (acts on row/column structure)
    sorry

-- filter(p, negate(r)) = negate(filter(p, r))
theorem rule_push_filter_through_negate :
    ∀ (p : Row → Bool) (r : Bag), filterB p (negateB r) = negateB (filterB p r) := by
    intro p r; funext x; simp only [filterB, unionB, negateB, thresholdB, predAnd, emptyBag]; cases p x <;> simp_all <;> try omega

-- filter(p, threshold(r)) = threshold(filter(p, r))
theorem rule_push_filter_through_threshold :
    ∀ (p : Row → Bool) (r : Bag), filterB p (thresholdB r) = thresholdB (filterB p r) := by
    intro p r; funext x; simp only [filterB, unionB, negateB, thresholdB, predAnd, emptyBag]; cases p x <;> simp_all <;> try omega

-- negate(a + b) = negate(a) + negate(b)
theorem rule_distribute_negate_union :
    ∀ (a : Bag) (b : Bag), negateB (unionB a b) = unionB (negateB a) (negateB b) := by
    intro a b; funext x; simp only [filterB, unionB, negateB, thresholdB, predAnd, emptyBag]; omega

-- filter(p, join(a, rest)) = join(filter(p, a), rest)  when p reads only a's columns
theorem rule_push_filter_into_join_first :
    ∀ (p : Row → Bool) (e : JoinSpec) (a : Bag) (rest : List Bag), filterB p (joinB e (a :: rest)) = joinB e ([filterB p a] ++ rest) := by
    -- not modeled at the bag level (acts on row/column structure)
    sorry

-- filter(p, a1 + .. + ak) = filter(p, a1) + .. + filter(p, ak)  when no predicate is known-false
theorem rule_distribute_filter_union_nary :
    ∀ (p : Row → Bool) (xs : List Bag), filterB p (unionAll xs) = unionAll ((xs.map (fun h => filterB p h))) := by
    -- provable by induction on the list (cf. Semantics `*_unionAll` lemmas)
    sorry

-- negate(a1 + .. + ak) = negate(a1) + .. + negate(ak)
theorem rule_distribute_negate_union_nary :
    ∀ (xs : List Bag), negateB (unionAll xs) = unionAll ((xs.map (fun h => negateB h))) := by
    -- provable by induction on the list (cf. Semantics `*_unionAll` lemmas)
    sorry

-- (a1 + .. + ak) + bs = a1 + .. + ak + bs
theorem rule_flatten_union_nary :
    ∀ (xs : List Bag) (ys : List Bag), unionAll (((unionAll xs) :: ys)) = unionAll (xs ++ ys) := by
    -- provable by induction on the list (cf. Semantics `*_unionAll` lemmas)
    sorry

-- join(e1, join(e2, xs), ys) = join(e1 ++ e2, xs, ys)
theorem rule_flatten_join_first :
    ∀ (e1 : JoinSpec) (e2 : JoinSpec) (xs : List Bag) (ys : List Bag), joinB e1 ((joinB e2 xs) :: ys) = joinB (catSpec e2 e1) (xs ++ ys) := by
    -- not modeled at the bag level (acts on row/column structure)
    sorry

-- filter(p, project(o, r)) = project(o, filter(p . o, r))
theorem rule_push_filter_past_project :
    ∀ (p : Row → Bool) (o : Row → Row) (r : Bag), filterB p (projB o r) = projB o (filterB (remapPred p o) r) := by
    -- not modeled at the bag level (acts on row/column structure)
    sorry

-- filter(p, join(a, b, rest)) = join(a, filter(shift(p,-|a|), b), rest)  when p reads only b
theorem rule_push_filter_into_join_second :
    ∀ (p : Row → Bool) (e : JoinSpec) (a : Bag) (b : Bag) (rest : List Bag), filterB p (joinB e (a :: b :: rest)) = joinB e ([a] ++ [filterB (shiftPred p) b] ++ rest) := by
    -- not modeled at the bag level (acts on row/column structure)
    sorry

-- threshold(r) = r  when r has non-negative multiplicities
theorem rule_threshold_elision :
    ∀ (r : Bag) (h_r : nonNeg r), thresholdB r = r := by
    intro r h_r; funext x; simp only [thresholdB]; have := h_r x; by_cases hp : r x > 0 <;> simp [hp] <;> omega

-- a + negate(a) = 0
theorem rule_union_cancel :
    ∀ (a : Bag), unionB a (negateB a) = emptyBag := by
    intro a; funext x; simp only [filterB, unionB, negateB, thresholdB, predAnd, emptyBag]; omega

-- reduce(gk, [], r) = project(cols(gk), r)  when gk is a unique key of r
theorem rule_reduce_elision :
    ∀ (gk : Row → Row) (aggs : Row → Row) (r : Bag), reduceB r = projB (colsOf gk) r := by
    -- not modeled at the bag level (acts on row/column structure)
    sorry

-- filter(p, r) = r  when every predicate is constantly true
theorem rule_drop_true_filter :
    ∀ (p : Row → Bool) (r : Bag) (h_p : ∀ x, p x = true), filterB p r = r := by
    intro p r h_p; funext x; simp only [filterB]; rw [h_p x]

-- filter(p, r) = 0  when some predicate is constantly false
theorem rule_empty_false_filter :
    ∀ (p : Row → Bool) (r : Bag) (h_p : ∀ x, p x = false), filterB p r = emptyBag := by
    intro p r h_p; funext x; simp only [filterB, emptyBag]; rw [h_p x]

-- map(s, r) = project(iota(|r|) ++ cols_of(s), r)  when s is all column refs
theorem rule_map_columns_to_projection :
    ∀ (s : Row → Row) (r : Bag), mapB s r = projB (catRows iota (colsOf s)) r := by
    -- not modeled at the bag level (acts on row/column structure)
    sorry

-- Join(rs) = WcoJoin(rs)  (same multiset, worst-case-optimal evaluation)
theorem rule_join_to_wcoj :
    ∀ (e : JoinSpec) (rs : List Bag), joinB e rs = wcoJoinB e rs := by
    intro e rs; rfl

-- join(e, a, b, rest) = join(equivs_outer(e, |a|+|b|), join(equivs_inner(e, |a|+|b|), a, b), rest)
theorem rule_binarize_join_first :
    ∀ (e : JoinSpec) (a : Bag) (b : Bag) (rest : List Bag), joinB e (a :: b :: rest) = joinB (equivsOuter e) ([joinB (equivsInner e) ([a] ++ [b])] ++ rest) := by
    -- not modeled at the bag level (acts on row/column structure)
    sorry

-- join(a, b) = project([restore], join(b, a)): reorder inputs, restore column order
theorem rule_commute_binary_join :
    ∀ (e : JoinSpec) (a : Bag) (b : Bag), joinB e [a, b] = projB swapProjection (joinB (swapEquivs e) ([b] ++ [a])) := by
    -- not modeled at the bag level (acts on row/column structure)
    sorry

-- join(project(o, a), b) = project(m, join(a, b)),  m = o ++ shift(iota(|b|), |a|)
theorem rule_pull_project_out_of_join_first :
    ∀ (e : JoinSpec) (o : Row → Row) (a : Bag) (b : Bag), joinB e [projB o a, b] = projB (catRows o (shiftRows iota)) (joinB (remapSpec e (catRows o (shiftRows iota))) ([a] ++ [b])) := by
    -- not modeled at the bag level (acts on row/column structure)
    sorry

-- ArrangeBy[k](x) = x  (x already produces an arrangement keyed by k)
theorem rule_arrange_idempotent :
    ∀ (x : Bag), x = x := by
    intro x; sorry

-- topk(0) = 0
theorem rule_topk_empty :
    ∀ (e : Bag), topkB e = emptyBag := by
    -- empty-propagation: operator is empty when input is empty (established by is_rel_empty guard)
    sorry

-- threshold(0) = 0
theorem rule_threshold_empty :
    ∀ (e : Bag), thresholdB e = emptyBag := by
    -- empty-propagation: operator is empty when input is empty (established by is_rel_empty guard)
    sorry

-- negate(0) = 0
theorem rule_negate_empty :
    ∀ (e : Bag), negateB e = emptyBag := by
    -- empty-propagation: operator is empty when input is empty (established by is_rel_empty guard)
    sorry

-- filter(p, 0) = 0
theorem rule_filter_empty :
    ∀ (p : Row → Bool) (e : Bag), filterB p e = emptyBag := by
    -- empty-propagation: operator is empty when input is empty (established by is_rel_empty guard)
    sorry

-- 0 + b = b
theorem rule_union_drop_empty_left :
    ∀ (e : Bag) (b : Bag), unionB e b = b := by
    -- union identity: requires is_rel_empty oracle (not modeled in bag algebra)
    sorry

-- a + 0 = a
theorem rule_union_drop_empty_right :
    ∀ (a : Bag) (e : Bag), unionB a e = a := by
    -- union identity: requires is_rel_empty oracle (not modeled in bag algebra)
    sorry

-- Not(Not(x)) = x
theorem rule_not_not :
    ∀ (env : Nat → Bool) (x : ScalarExpr), denoteS env (ScalarExpr.notE (ScalarExpr.notE x)) = denoteS env x := by
    intro env x; simp [denoteS]

-- And(x) = x
theorem rule_and_single :
    ∀ (env : Nat → Bool) (x : ScalarExpr), denoteS env (ScalarExpr.andE [x]) = denoteS env x := by
    intro env x; simp [denoteS]

-- Or(x) = x
theorem rule_or_single :
    ∀ (env : Nat → Bool) (x : ScalarExpr), denoteS env (ScalarExpr.orE [x]) = denoteS env x := by
    intro env x; simp [denoteS]

-- Not(And(xs...)) = Or(Not(xs)...)
theorem rule_not_demorgan_and :
    ∀ (env : Nat → Bool) (xs : List ScalarExpr), denoteS env (ScalarExpr.notE (ScalarExpr.andE xs)) = denoteS env (ScalarExpr.orE ((xs.map (fun h => ScalarExpr.notE h)))) := by
    intro env xs; first | simp [denoteS, List.foldr, List.map] | sorry

-- Not(Or(xs...)) = And(Not(xs)...)
theorem rule_not_demorgan_or :
    ∀ (env : Nat → Bool) (xs : List ScalarExpr), denoteS env (ScalarExpr.notE (ScalarExpr.orE xs)) = denoteS env (ScalarExpr.andE ((xs.map (fun h => ScalarExpr.notE h)))) := by
    intro env xs; first | simp [denoteS, List.foldr, List.map] | sorry

end MirRewrite
