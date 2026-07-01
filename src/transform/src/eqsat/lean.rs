// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Generate a Lean 4 specification from the rewrite DSL.
//!
//! For every [`Rule`] we emit a theorem asserting that the denotations (under
//! the multiplicity semantics in `lean/MirRewrite/Semantics.lean`) of the
//! left- and right-hand sides are equal. The pattern/template structure
//! translates mechanically to the semantic combinators; metavariables become
//! universally-quantified parameters.
//!
//! Proofs are selected by the *shape* of the translated statement:
//!
//! * statements over the fully-modeled combinators (`filterB`, `unionB`,
//!   `negateB`, `thresholdB`, ...) get a discharging tactic;
//! * statements mentioning the deliberately-opaque `mapB`/`projB` (whose
//!   algebraic laws are not modeled at the bag level) are emitted with `sorry`
//!   and a note, so the obligation is explicit rather than hidden.

use std::collections::BTreeMap;
use std::fmt::Write;

use super::dsl::*;

/// Render the full `Generated.lean` file for a rule set.
pub fn emit_lean(rules: &RuleSet) -> String {
    let mut out = String::new();
    out.push_str(HEADER);
    for rule in &rules.rules {
        out.push_str(&emit_rule(rule));
        out.push('\n');
    }
    out.push_str("end MirRewrite\n");
    out
}

const HEADER: &str = r#"-- Copyright Materialize, Inc. and contributors. All rights reserved.
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

"#;

fn emit_rule(rule: &Rule) -> String {
    // Collect metavariables and their Lean types from the LHS (which binds all
    // of them), in order of first appearance.
    let mut binders: Vec<(String, &'static str)> = Vec::new();
    let mut seen = BTreeMap::new();
    collect_binders(&rule.lhs, &mut binders, &mut seen);

    let lhs = translate_pat(&rule.lhs);
    let rhs = translate_tmpl(&rule.rhs, "h");

    // Model `non_negative(r)` side conditions as Lean hypotheses, so the
    // generated statement is the (true) conditional theorem rather than a
    // false unconditional one.
    let nonneg: Vec<&str> = rule
        .conds
        .iter()
        .filter_map(|c| match c {
            Cond::NonNegative { rel } => Some(rel.as_str()),
            _ => None,
        })
        .collect();
    // Scalar-structure conditions become hypotheses on the predicate function:
    // `all_true(p)` => `forall x, p x = true` (filter is identity), `any_false(p)` =>
    // `forall x, p x = false` (filter is empty -- a conjunction with a false conjunct
    // is everywhere false).
    let all_true = first_payload(rule, |c| matches!(c, Cond::AllTrue { .. }));
    let any_false = first_payload(rule, |c| matches!(c, Cond::AnyFalse { .. }));

    let mut hyps: Vec<(String, String)> = nonneg
        .iter()
        .map(|r| (format!("h_{r}"), format!("nonNeg {r}")))
        .collect();
    if let Some(p) = &all_true {
        hyps.push((format!("h_{p}"), format!("∀ x, {p} x = true")));
    }
    if let Some(p) = &any_false {
        hyps.push((format!("h_{p}"), format!("∀ x, {p} x = false")));
    }

    // True when the rule is guarded by `is_rel_empty`, signalling an
    // empty-propagation rule whose proof requires the `IsRelEmpty` oracle.
    let is_empty_prop = rule
        .conds
        .iter()
        .any(|c| matches!(c, Cond::IsRelEmpty { .. }));

    let proof = choose_proof(
        &lhs,
        &rhs,
        &binders,
        &hyps,
        nonneg.first().copied(),
        all_true.as_deref(),
        any_false.as_deref(),
        is_empty_prop,
    );

    let quantifier = if binders.is_empty() && hyps.is_empty() {
        String::new()
    } else {
        let mut s = String::from("∀ ");
        for (name, ty) in &binders {
            let _ = write!(s, "({name} : {ty}) ");
        }
        for (name, ty) in &hyps {
            let _ = write!(s, "({name} : {ty}) ");
        }
        format!("{}, ", s.trim_end())
    };

    let doc = rule
        .doc
        .as_deref()
        .map(|d| format!("-- {d}\n"))
        .unwrap_or_default();

    format!(
        "{doc}theorem rule_{name} :\n    {quantifier}{lhs} = {rhs} := {proof}\n",
        name = rule.name,
    )
}

fn collect_binders(
    pat: &Pat,
    out: &mut Vec<(String, &'static str)>,
    seen: &mut BTreeMap<String, ()>,
) {
    let add = |name: &str,
               ty: &'static str,
               out: &mut Vec<(String, &'static str)>,
               seen: &mut BTreeMap<String, ()>| {
        if seen.insert(name.to_string(), ()).is_none() {
            out.push((name.to_string(), ty));
        }
    };
    match pat {
        Pat::RelVar(name) => add(name, "Bag", out, seen),
        Pat::Filter { preds, input } => {
            add(preds, "Row → Bool", out, seen);
            collect_binders(input, out, seen);
        }
        Pat::Map { scalars, input } => {
            add(scalars, "Row → Row", out, seen);
            collect_binders(input, out, seen);
        }
        Pat::Project { outputs, input } => {
            add(outputs, "Row → Row", out, seen);
            collect_binders(input, out, seen);
        }
        Pat::Reduce {
            group_key,
            aggregates,
            input,
        } => {
            add(group_key, "Row → Row", out, seen);
            add(aggregates, "Row → Row", out, seen);
            collect_binders(input, out, seen);
        }
        Pat::FlatMap { func, exprs, input } => {
            add(func, "TableFunc", out, seen);
            add(exprs, "Row → Row", out, seen);
            collect_binders(input, out, seen);
        }
        Pat::Negate(input) | Pat::Threshold(input) | Pat::TopK(input) => {
            collect_binders(input, out, seen)
        }
        // ArrangeBy is the identity bag, so its key binds nothing the Lean
        // statement uses; only its input contributes binders.
        Pat::ArrangeBy { input, .. } => collect_binders(input, out, seen),
        Pat::Join {
            equivalences,
            inputs,
        }
        | Pat::WcoJoin {
            equivalences,
            inputs,
        } => {
            add(equivalences, "JoinSpec", out, seen);
            for i in &inputs.items {
                collect_binders(i, out, seen);
            }
            if let Some(rest) = &inputs.rest {
                add(rest, "List Bag", out, seen);
            }
        }
        Pat::Union { inputs } => {
            for i in &inputs.items {
                collect_binders(i, out, seen);
            }
            if let Some(rest) = &inputs.rest {
                add(rest, "List Bag", out, seen);
            }
        }
    }
}

/// Wrap an expression in parentheses if it is compound (so it is safe as a
/// function argument); leave bare identifiers alone.
fn arg(s: String) -> String {
    if s.contains(' ') { format!("({s})") } else { s }
}

fn translate_pat(pat: &Pat) -> String {
    match pat {
        Pat::RelVar(n) => n.clone(),
        Pat::Filter { preds, input } => format!("filterB {preds} {}", arg(translate_pat(input))),
        Pat::Map { scalars, input } => format!("mapB {scalars} {}", arg(translate_pat(input))),
        Pat::Project { outputs, input } => {
            format!("projB {outputs} {}", arg(translate_pat(input)))
        }
        Pat::Reduce { input, .. } => format!("reduceB {}", arg(translate_pat(input))),
        Pat::FlatMap { input, .. } => format!("flatMapB {}", arg(translate_pat(input))),
        Pat::Negate(input) => format!("negateB {}", arg(translate_pat(input))),
        Pat::Threshold(input) => format!("thresholdB {}", arg(translate_pat(input))),
        // TopK is opaque; rules touching it use `is_rel_empty` guards which
        // yield `emptyBag` on both sides, so the proof is `rfl`.
        Pat::TopK(input) => format!("topkB {}", arg(translate_pat(input))),
        // ArrangeBy is the identity on the bag (a physical arrangement does not
        // change rows), so it translates to its input directly; the idempotence
        // rule's two sides then coincide and the proof is `rfl`.
        Pat::ArrangeBy { input, .. } => translate_pat(input),
        Pat::Join {
            equivalences,
            inputs,
        } => {
            format!("joinB {equivalences} {}", pat_list(inputs))
        }
        Pat::WcoJoin {
            equivalences,
            inputs,
        } => {
            format!("wcoJoinB {equivalences} {}", pat_list(inputs))
        }
        Pat::Union { inputs } => {
            // All-literal binary unions keep the right-nested fold (provable);
            // a variadic `Union(xs...)` becomes `unionAll` over a `List Bag`.
            if inputs.rest.is_none() {
                union_fold(&inputs.items.iter().map(translate_pat).collect::<Vec<_>>())
            } else {
                let items: Vec<String> = inputs.items.iter().map(translate_pat).collect();
                format!(
                    "unionAll {}",
                    arg(lean_list(&items, inputs.rest.as_deref()))
                )
            }
        }
    }
}

/// Translate a template. `hole` is the Lean variable name bound to `_` inside
/// an enclosing `map(...)` list combinator.
fn translate_tmpl(t: &Tmpl, hole: &str) -> String {
    match t {
        Tmpl::RelVar(n) => n.clone(),
        Tmpl::Hole => hole.to_string(),
        // `Empty(r)` is the zero-row constant with `r`'s arity; in the bag
        // model it is `emptyBag` regardless of `r`.
        Tmpl::Empty(_) => "emptyBag".to_string(),
        Tmpl::Filter { preds, input } => {
            format!(
                "filterB {} {}",
                translate_pexpr(preds, Kind::Pred),
                arg(translate_tmpl(input, hole))
            )
        }
        Tmpl::Map { scalars, input } => {
            format!(
                "mapB {} {}",
                translate_pexpr(scalars, Kind::Rows),
                arg(translate_tmpl(input, hole))
            )
        }
        Tmpl::Project { outputs, input } => {
            format!(
                "projB {} {}",
                translate_pexpr(outputs, Kind::Rows),
                arg(translate_tmpl(input, hole))
            )
        }
        Tmpl::Reduce { input, .. } => format!("reduceB {}", arg(translate_tmpl(input, hole))),
        Tmpl::FlatMap { input, .. } => format!("flatMapB {}", arg(translate_tmpl(input, hole))),
        Tmpl::Negate(input) => format!("negateB {}", arg(translate_tmpl(input, hole))),
        Tmpl::Threshold(input) => format!("thresholdB {}", arg(translate_tmpl(input, hole))),
        Tmpl::Join {
            equivalences,
            inputs,
        } => {
            format!(
                "joinB {} {}",
                translate_pexpr(equivalences, Kind::Spec),
                arg(tmpl_list_expr(inputs, hole))
            )
        }
        Tmpl::WcoJoin {
            equivalences,
            inputs,
        } => {
            format!(
                "wcoJoinB {} {}",
                translate_pexpr(equivalences, Kind::Spec),
                arg(tmpl_list_expr(inputs, hole))
            )
        }
        Tmpl::Union { inputs } => {
            // If every element is a literal item, keep the right-nested
            // `unionB` fold (so binary-union rules stay `rfl`/provable);
            // otherwise fall back to `unionAll` over a `List Bag`.
            let all_items: Option<Vec<String>> = inputs
                .elems
                .iter()
                .map(|e| match e {
                    TElem::Item(t) => Some(translate_tmpl(t, hole)),
                    _ => None,
                })
                .collect();
            match all_items {
                Some(items) => union_fold(&items),
                None => format!("unionAll {}", arg(tmpl_list_expr(inputs, hole))),
            }
        }
    }
}

/// Build a Lean `List Bag` expression from a template input list, concatenating
/// items, spliced rests, and mapped rests with `++`.
fn tmpl_list_expr(list: &ListTmpl, hole: &str) -> String {
    let parts: Vec<String> = list
        .elems
        .iter()
        .map(|e| match e {
            TElem::Item(t) => format!("[{}]", translate_tmpl(t, hole)),
            TElem::Splice(name) => name.clone(),
            // A fresh hole name `h` for the mapped element (no nested maps in
            // the rule set, so a single name suffices).
            TElem::MapSplice { func, list } => {
                format!("({list}.map (fun h => {}))", translate_tmpl(func, "h"))
            }
        })
        .collect();
    if parts.len() == 1 {
        parts.into_iter().next().unwrap()
    } else {
        parts.join(" ++ ")
    }
}

/// The Lean type of a payload, so combinators translate to the right (typed)
/// Lean function.
#[derive(Clone, Copy)]
enum Kind {
    /// `Row -> Bool` (filter predicates).
    Pred,
    /// `Row -> Row` (map/project column lists).
    Rows,
    /// `JoinSpec` (join equivalences).
    Spec,
}

fn translate_pexpr(e: &PExpr, kind: Kind) -> String {
    match e {
        PExpr::Var(n) => n.clone(),
        PExpr::Concat(a, b) => {
            // Concatenation of two same-kind payload lists. `predAnd` is the
            // only one we reason about; the column-list version is opaque
            // (its rules are `sorry`), but must still be well-typed.
            let op = match kind {
                Kind::Pred => "predAnd",
                Kind::Rows => "catRows",
                Kind::Spec => "catSpec",
            };
            format!(
                "({op} {} {})",
                translate_pexpr(a, kind),
                translate_pexpr(b, kind)
            )
        }
        PExpr::Compose(a, b) => {
            format!(
                "(projCompose {} {})",
                translate_pexpr(a, kind),
                translate_pexpr(b, kind)
            )
        }
        // `shift` and `remap` rewrite column indices; we do not model that at
        // the bag level, so they are opaque (and appear only in `sorry`-ed
        // obligations), but must be well-typed.
        PExpr::Shift(p, _k) => {
            let op = match kind {
                Kind::Pred => "shiftPred",
                Kind::Rows => "shiftRows",
                Kind::Spec => "shiftSpec",
            };
            format!("({op} {})", translate_pexpr(p, kind))
        }
        PExpr::Remap(p, outs) => {
            let op = match kind {
                Kind::Pred => "remapPred",
                Kind::Rows => "remapRows",
                Kind::Spec => "remapSpec",
            };
            // The remapping (`outs`) is always a projection (`Row -> Row`).
            format!(
                "({op} {} {})",
                translate_pexpr(p, kind),
                translate_pexpr(outs, Kind::Rows)
            )
        }
        // A group key reinterpreted as a projection (opaque; appears only in a
        // `sorry`-ed `Project`-based obligation).
        PExpr::ColsOf(p) => format!("(colsOf {})", translate_pexpr(p, Kind::Rows)),
        // The identity projection `[0..n]` -- opaque at the bag level (it acts on
        // column structure), appears only in a `sorry`-ed `Project` obligation.
        PExpr::Iota(_) => "iota".to_string(),
        // Equivalence-splitting and swapping operate on the join spec; they are
        // opaque at the bag level (appear only in `sorry`-ed obligations).
        PExpr::EquivsInner(p, _) => format!("(equivsInner {})", translate_pexpr(p, Kind::Spec)),
        PExpr::EquivsOuter(p, _) => format!("(equivsOuter {})", translate_pexpr(p, Kind::Spec)),
        PExpr::SwapEquivs(p, _, _) => format!("(swapEquivs {})", translate_pexpr(p, Kind::Spec)),
        // The restore projection for join commutativity acts on column structure;
        // opaque at the bag level (appears only in a `sorry`-ed `Project` obligation).
        PExpr::SwapProjection(_, _) => "swapProjection".to_string(),
    }
}

/// Right-nested fold of a list of bags with `unionB`. A singleton is itself.
fn union_fold(items: &[String]) -> String {
    match items.split_first() {
        None => "emptyBag".to_string(),
        Some((head, [])) => head.clone(),
        Some((head, tail)) => {
            format!("unionB {} {}", arg(head.clone()), arg(union_fold(tail)))
        }
    }
}

fn pat_list(inputs: &ListPat) -> String {
    let items: Vec<String> = inputs.items.iter().map(translate_pat).collect();
    lean_list(&items, inputs.rest.as_deref())
}

/// Render a Lean `List Bag` from explicit `items` and an optional `rest` tail.
fn lean_list(items: &[String], rest: Option<&str>) -> String {
    match rest {
        None => format!("[{}]", items.join(", ")),
        Some(r) => {
            if items.is_empty() {
                r.to_string()
            } else {
                let cons: String = items
                    .iter()
                    .map(|i| format!("{} :: ", arg(i.clone())))
                    .collect();
                format!("({cons}{r})")
            }
        }
    }
}

/// Pick a proof tactic from the shape of the statement. `binders` carries each
/// metavariable's Lean type so we can case-split on the `Row -> Bool`
/// predicates; `hyps` are the extra hypotheses (e.g. `nonNeg r`) and
/// `nonneg_rel` names the relation a `non_negative` condition applies to.
/// The first condition payload matching `pick`, if any.
fn first_payload(rule: &Rule, pick: impl Fn(&Cond) -> bool) -> Option<String> {
    rule.conds.iter().find(|c| pick(c)).and_then(|c| match c {
        Cond::AllTrue { payload } | Cond::AnyFalse { payload } | Cond::AllColumns { payload } => {
            Some(payload.clone())
        }
        _ => None,
    })
}

#[allow(clippy::too_many_arguments)]
fn choose_proof(
    lhs: &str,
    rhs: &str,
    binders: &[(String, &'static str)],
    hyps: &[(String, String)],
    nonneg_rel: Option<&str>,
    all_true: Option<&str>,
    any_false: Option<&str>,
    is_empty_prop: bool,
) -> String {
    let both = format!("{lhs} {rhs}");
    let mut intros: Vec<&str> = binders.iter().map(|(n, _)| n.as_str()).collect();
    intros.extend(hyps.iter().map(|(n, _)| n.as_str()));
    let intro = if intros.is_empty() {
        String::new()
    } else {
        format!("intro {}; ", intros.join(" "))
    };

    // A filter by an everywhere-true predicate is the identity; by an
    // everywhere-false predicate it is empty. `filterB p b = fun x => cond (p x)
    // (b x) 0`, so rewriting `p x` collapses the `cond` definitionally.
    if let Some(p) = all_true {
        if both.contains("filterB") {
            return format!("by\n    {intro}funext x; simp only [filterB]; rw [h_{p} x]");
        }
    }
    if let Some(p) = any_false {
        if both.contains("filterB") {
            return format!("by\n    {intro}funext x; simp only [filterB, emptyBag]; rw [h_{p} x]");
        }
    }

    // Threshold elision under a `nonNeg` hypothesis: keeping positive-count
    // rows is the identity when no count is negative.
    if both.contains("thresholdB") {
        if let Some(r) = nonneg_rel {
            return format!(
                "by\n    {intro}funext x; simp only [thresholdB]; have := h_{r} x; by_cases hp : {r} x > 0 <;> simp [hp] <;> omega"
            );
        }
    }

    // Empty-propagation rules (guarded by `is_rel_empty`): the operator
    // returns empty when its input is the empty relation. Not provable from the
    // bag algebra alone -- the `is_rel_empty` guard is the oracle. Mark as
    // `sorry` so the obligation is explicit.
    if is_empty_prop && rhs == "emptyBag" {
        return "by\n    -- empty-propagation: operator is empty when input is empty (established by is_rel_empty guard)\n    sorry"
            .to_string();
    }
    // Union identity rules (union_drop_empty_left / union_drop_empty_right):
    // the proof requires knowing one summand is `emptyBag`, which is not
    // expressible without the `is_rel_empty` oracle.
    if is_empty_prop && lhs.starts_with("unionB") {
        return "by\n    -- union identity: requires is_rel_empty oracle (not modeled in bag algebra)\n    sorry"
            .to_string();
    }

    // N-ary list laws (`unionAll`, `map`) are provable by induction on the
    // list -- see the `*_unionAll` lemmas in Semantics.lean -- but we do not
    // synthesize induction here, so leave the obligation explicit.
    if both.contains("unionAll") {
        return "by\n    -- provable by induction on the list (cf. Semantics `*_unionAll` lemmas)\n    sorry"
            .to_string();
    }
    // Join == WcoJoin is definitional (check before the opaque-join guard).
    if both.contains("wcoJoinB") {
        return format!("by\n    {intro}rfl");
    }
    // Opaque operators we do not model algebraically => leave the obligation
    // explicit. (map/project/reduce act on row column-structure; a plain join
    // is opaque here.)
    if ["mapB", "projB", "reduceB", "joinB"]
        .iter()
        .any(|o| both.contains(o))
    {
        return "by\n    -- not modeled at the bag level (acts on row/column structure)\n    sorry"
            .to_string();
    }

    let simp = "simp only [filterB, unionB, negateB, thresholdB, predAnd, emptyBag]";

    // Pure union reassociation is definitional under the right-nested fold.
    if both.contains("unionB")
        && !both.contains("filterB")
        && !both.contains("negateB")
        && !both.contains("thresholdB")
    {
        return format!("by\n    {intro}rfl");
    }

    // Predicate binders to case-split on.
    let preds: Vec<&str> = binders
        .iter()
        .filter(|(_, ty)| *ty == "Row → Bool")
        .map(|(n, _)| n.as_str())
        .collect();

    if !preds.is_empty() {
        let cases: String = preds.iter().map(|p| format!("cases {p} x <;> ")).collect();
        return format!("by\n    {intro}funext x; {simp}; {cases}simp_all <;> try omega");
    }
    // Threshold idempotence: split on the (single) bag's sign.
    if both.contains("thresholdB") {
        let r = intros.first().copied().unwrap_or("r");
        return format!("by\n    {intro}funext x; {simp}; by_cases h : {r} x > 0 <;> simp [h]");
    }
    // Negation laws are linear arithmetic.
    if both.contains("negateB") {
        return format!("by\n    {intro}funext x; {simp}; omega");
    }
    format!("by\n    {intro}sorry")
}
