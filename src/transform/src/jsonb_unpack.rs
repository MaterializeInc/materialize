// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Replaces multiple jsonb accessors on a common value with a single
//! multi-field unpacking table function, [`TableFunc::JsonbUnpack`].
//!
//! `k` scalar accessors (`->`, `->>`, `#>`, `#>>`) on the same jsonb value
//! each re-scan it from the start, so a SELECT list extracting `k` fields
//! from one object does `O(k * |object|)` work per row. `JsonbUnpack`
//! extracts all the fields in a single pass.
//!
//! # What the transform does
//!
//! **Where it looks.** Two kinds of *sites*, processed independently:
//!
//! 1. every maximal Map/Filter/Project chain, treated as one
//!    [`MapFilterProject`];
//! 2. every `Reduce`'s group key and aggregate argument expressions, treated
//!    as one expression bag. (By this phase of the pipeline, accessors in
//!    GROUP BY and aggregate arguments live *inside* the Reduce:
//!    `ReductionPushdown` absorbs Maps into Reduces in the logical pipeline.)
//!
//! Nothing else: not FlatMap arguments, TopK limits, Join equivalences, or
//! ArrangeBy keys; MFPs already pushed into source imports are out of reach
//! by pipeline position. Accessors in different sites never share an unpack.
//!
//! **What fires.** Within a site, after running the existing scalar ANF
//! ([`MapFilterProject::memoize_expressions`]) on the bag:
//!
//! - A *candidate* is a memoized expression `<column> -> <literal>` (or
//!   `->>`, `#>`, `#>>`) with a non-null literal key/index/path. Non-literal
//!   keys are out of scope (no batched eval is possible without knowing the
//!   fields up front; a future extension could handle keys that are at least
//!   common across the site). Empty paths (`#> '{}'`, the identity) and
//!   paths with null elements (constant NULL) are degenerate and skipped.
//!   Accessors under `CASE` are out of scope as a consequence of reusing the
//!   ANF: `eager_children` deliberately does not descend into `If` branches
//!   (nor `COALESCE` tails), so such calls never become candidates.
//! - At MFP sites, candidates transitively needed by a Filter predicate are
//!   *pinned*: they stay below the unpacking, in their original scalar form,
//!   so that predicates remain directly above the site's input where join
//!   closures, arrangement-read fusion, and similar mechanisms consume them.
//!   A pinned accessor that is also selected does not count toward the
//!   threshold below. (Reduce sites have no predicates.)
//! - Unpinned candidates are grouped by base column. A group **fires** iff
//!   it has at least [`MIN_FIELDS_PER_UNPACK`] distinct fields and its base
//!   is *below-computable*:
//!   - any expression over input columns and pinned/below-computable columns
//!     (materialized below the FlatMap stack if not already a column) — this
//!     includes computed bases such as a string-to-jsonb cast; or
//!   - a raw (`->`) field of another firing group, or an accessor over
//!     below-computable columns — these bridge between stacked FlatMaps (the
//!     latter inlined into the FlatMap argument expression).
//!
//!   Equivalently, the only excluded bases are those that depend on another
//!   unpack's output through anything other than raw accessor steps;
//!   supporting them would require Map layers interleaved between FlatMaps —
//!   which is exactly what running this transform to a fixpoint would
//!   produce (safe and terminating, see `skip_site`), should the need arise.
//!
//!   Note on chains: the ANF blows `a -> 'x' -> 'y'` into per-link
//!   expressions with *different* bases; each link is grouped and fired
//!   independently, and intermediate links count toward their base's group.
//!
//! **Output shape.** A fired MFP site is rebuilt as:
//!
//! ```text
//! <input>
//!   [Map + Filter]        — predicate support (in original scalar form) and
//!                           computed bases; predicates evaluate before bases
//!   FlatMap JsonbUnpack   — one per firing group, stacked in dependency order
//!   [ FlatMap JsonbUnpack … ]
//!   [Map] + [Project]     — leftover expressions rewritten over the unpacked
//!                           columns + the site's original projection
//! ```
//!
//! At a fired Reduce site the FlatMap stack (plus a Map for computed bases)
//! is spliced directly below the Reduce, and the rewritten expressions go
//! back into the group key and aggregate arguments.
//!
//! A site where nothing fires is left **byte-for-byte unchanged**.
//!
//! **Why this is safe.** `JsonbUnpack` is exactly 1:1, infallible, and
//! accessor-exact (see its documentation), so inserting it into a linear
//! chain preserves cardinalities, multiplicities, unique keys, and
//! monotonicity. Hoisting candidates above other map expressions cannot
//! introduce errors (accessors are infallible; conditionally-evaluated calls
//! never become candidates). Moving computed bases below the stack is
//! error-safe because there they evaluate after *all* of the site's
//! predicates, i.e., on a subset of the rows they used to see.
//!
//! # Why didn't it fire?
//!
//! For debugging: the feature flag is off; the query is a fast-path peek
//! (this transform is not part of the fast-path optimizer); the key is not a
//! literal (or is an empty path); fewer than [`MIN_FIELDS_PER_UNPACK`]
//! distinct fields on the same base within one site (chains count per link);
//! the accessors are used by filters (pinned — even if also selected); the
//! accessor is under a `CASE`; the accessor is in an unsupported position
//! (FlatMap argument / TopK limit / Join equivalence / ArrangeBy key /
//! source-pushed MFP); the base depends on an unpack output via a
//! non-accessor expression; the accessors sit below, or next to, a literal
//! error in their chain (literal errors are never rearranged).

use std::collections::{BTreeMap, BTreeSet};

use itertools::Itertools;
use mz_expr::{
    BinaryFunc, Columns, JsonbUnpackField, JsonbUnpackFieldKind, MapFilterProject, MirRelationExpr,
    MirScalarExpr, TableFunc,
};
use mz_repr::Datum;

use crate::canonicalize_mfp::CanonicalizeMfp;
use crate::{Transform, TransformCtx, TransformError};

/// The minimum number of distinct fields that must share a base before the
/// group is rewritten to a `JsonbUnpack`. With a single field the scalar
/// accessor does the same single scan without the FlatMap overhead.
pub const MIN_FIELDS_PER_UNPACK: usize = 2;

/// Replaces multiple jsonb accessors on a common value with a single
/// multi-field unpacking table function. See the module documentation.
#[derive(Debug)]
pub struct JsonbUnpack;

impl Transform for JsonbUnpack {
    fn name(&self) -> &'static str {
        "JsonbUnpack"
    }

    #[mz_ore::instrument(
        target = "optimizer",
        level = "debug",
        fields(path.segment = "jsonb_unpack")
    )]
    fn actually_perform_transform(
        &self,
        relation: &mut MirRelationExpr,
        _ctx: &mut TransformCtx,
    ) -> Result<(), TransformError> {
        let mut todo: Vec<&mut MirRelationExpr> = vec![relation];
        while let Some(expr) = todo.pop() {
            match expr {
                MirRelationExpr::Map { .. }
                | MirRelationExpr::Filter { .. }
                | MirRelationExpr::Project { .. } => {
                    todo.push(process_mfp_site(expr));
                }
                MirRelationExpr::Reduce { .. } => {
                    process_reduce_site(expr);
                    // The Reduce's new input may start with FlatMaps we just
                    // built; the Map below them merges with whatever chain
                    // was below the Reduce into one site, processed next.
                    let MirRelationExpr::Reduce { input, .. } = expr else {
                        unreachable!()
                    };
                    todo.push(skip_flat_maps(input));
                }
                other => todo.extend(other.children_mut()),
            }
        }
        mz_repr::explain::trace_plan(&*relation);
        Ok(())
    }
}

/// Descends through the layers making up a (possibly rewritten) MFP site —
/// Map/Filter/Project nodes and `JsonbUnpack` FlatMaps — and returns the
/// site's input, where the traversal continues.
///
/// This is purely a traversal optimization, not a correctness requirement:
/// if it stopped too early, the skipped layers would be processed as sites
/// of their own, which is safe. Processing this transform's own output never
/// undoes or re-wraps what it built (unpacked accessors are gone from the
/// expressions, predicate support gets re-pinned); it can only find
/// *legitimately* remaining work — in particular, a group excluded because
/// its base depended on an unpack output through a non-accessor expression
/// becomes unpackable once that expression is an ordinary map expression of
/// the rewritten upper layer. So running the transform to a fixpoint would
/// be safe (each firing strictly decreases the number of accessor calls in
/// sites, so it terminates) and would even lift the interleaved-base
/// limitation; a single pass just doesn't chase that rare case.
fn skip_site(expr: &mut MirRelationExpr) -> &mut MirRelationExpr {
    let mut cur = expr;
    loop {
        match cur {
            MirRelationExpr::Map { input, .. }
            | MirRelationExpr::Filter { input, .. }
            | MirRelationExpr::Project { input, .. }
            | MirRelationExpr::FlatMap {
                input,
                func: TableFunc::JsonbUnpack { .. },
                ..
            } => cur = input,
            other => return other,
        }
    }
}

/// Descends through `JsonbUnpack` FlatMaps only (used below a rewritten
/// Reduce, where the Map below the FlatMaps merges with the chain below into
/// a site of its own).
fn skip_flat_maps(expr: &mut MirRelationExpr) -> &mut MirRelationExpr {
    let mut cur = expr;
    loop {
        match cur {
            MirRelationExpr::FlatMap {
                input,
                func: TableFunc::JsonbUnpack { .. },
                ..
            } => cur = input,
            other => return other,
        }
    }
}

/// Treats the maximal Map/Filter/Project chain rooted at `expr` as one site,
/// rewrites it in place if any unpack group fires, and returns the node
/// below the (possibly rebuilt) site, where the traversal continues.
fn process_mfp_site(expr: &mut MirRelationExpr) -> &mut MirRelationExpr {
    // The mutable-reference extraction hands back both the site's MFP and
    // the exact node its walk stopped at, so the MFP we plan against and the
    // input we splice onto cannot disagree. Note that despite the name, this
    // extraction only *copies* the chain into an MFP — the tree is untouched
    // (unlike its destructive sibling `extract_non_errors_from_expr_mut`),
    // so when nothing fires the site simply remains as is. (This variant
    // also stops at operators containing literal errors, which we don't
    // want to rearrange anyway; `plan_site` separately declines literal
    // errors nested inside larger expressions.)
    let (mfp, input_ref) = MapFilterProject::extract_non_errors_from_expr_ref_mut(expr);
    if let Some(plan) = plan_site(&mfp) {
        let input = input_ref.take_dangerous();
        let mut new = plan.install(input);
        CanonicalizeMfp::rebuild_mfp(plan.upper_mfp, &mut new);
        *expr = new;
    }
    skip_site(expr)
}

/// Treats the group key and aggregate argument expressions of the Reduce at
/// `expr` as one site and rewrites the Reduce in place if any unpack group
/// fires.
///
/// This per-operator handling could be generalized: instead of teaching this
/// transform about each operator that embeds scalar expressions, a pair of
/// transforms could (a) pull all scalar expressions out of such operators
/// into Maps, and (b) push them back in afterwards. The push-back half does
/// not exist today — `ReductionPushdown` absorbs only a bare Map, and a
/// robust version would have to consume a whole MFP and absorb only the map
/// expressions not needed by the MFP's own Filter/Project parts. Note that
/// an MFP left *below* a Reduce lowers to a standalone operator: Reduce only
/// fuses MFPs from above (`enable_reduce_mfp_fusion`).
fn process_reduce_site(expr: &mut MirRelationExpr) {
    let MirRelationExpr::Reduce {
        input,
        group_key,
        aggregates,
        ..
    } = expr
    else {
        unreachable!("process_reduce_site called on a non-Reduce");
    };
    if group_key.is_empty() && aggregates.is_empty() {
        return;
    }
    let input_arity = input.arity();
    let bag: Vec<MirScalarExpr> = group_key
        .iter()
        .chain(aggregates.iter().map(|agg| &agg.expr))
        .cloned()
        .collect();
    let bag_len = bag.len();
    let mfp = MapFilterProject::new(input_arity)
        .map(bag)
        .project(input_arity..input_arity + bag_len);
    let Some(plan) = plan_site(&mfp) else {
        return;
    };
    // The rewritten expressions go back into the Reduce as standalone
    // expressions (duplicating any shared subexpressions, exactly as
    // `ReductionPushdown` does when absorbing a Map).
    let rewritten = standalone_exprs(&plan.upper_mfp);
    let inner = input.take_dangerous();
    **input = plan.install(inner);
    let key_len = group_key.len();
    for (key, rewritten) in group_key.iter_mut().zip_eq(&rewritten[..key_len]) {
        *key = rewritten.clone();
    }
    for (agg, rewritten) in aggregates.iter_mut().zip_eq(&rewritten[key_len..]) {
        agg.expr = rewritten.clone();
    }
}

/// The planned rewrite of one site.
struct SitePlan {
    /// The layer to install directly on the site's input: the site's
    /// predicates with their support (in original scalar form), the computed
    /// bases, and a projection passing through the input columns plus
    /// whatever the layers above demand. Predicates evaluate before any
    /// computed base (pinned expressions precede bases in the map, and the
    /// predicates' support is contained in the pinned prefix).
    lower_mfp: MapFilterProject,
    /// The unpack FlatMaps in stack order (bottom first); each argument is in
    /// terms of the layout below that FlatMap.
    flat_maps: Vec<(Vec<JsonbUnpackField>, MirScalarExpr)>,
    /// The site's outputs over the post-stack layout
    /// `[input columns] ++ [demanded lower expressions] ++ [unpacked fields]`.
    upper_mfp: MapFilterProject,
}

impl SitePlan {
    /// Builds the lower MFP and the FlatMap stack on top of `input`. The
    /// caller is responsible for the upper layer (`upper_mfp`), whose form
    /// differs between MFP and Reduce sites.
    fn install(&self, input: MirRelationExpr) -> MirRelationExpr {
        let mut rel = input;
        CanonicalizeMfp::rebuild_mfp(self.lower_mfp.clone(), &mut rel);
        for (fields, arg) in &self.flat_maps {
            rel = MirRelationExpr::FlatMap {
                input: Box::new(rel),
                func: TableFunc::JsonbUnpack {
                    fields: fields.clone(),
                },
                exprs: vec![arg.clone()],
            };
        }
        rel
    }
}

/// How a memoized column is available to expressions above it.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum State {
    /// An input column or a pinned expression: materialized below the stack.
    Below,
    /// A non-candidate expression over Below/BelowComputable columns: *can*
    /// be materialized below the stack, but only is if some firing group's
    /// base needs it.
    BelowComputable,
    /// An unfired candidate over a usable base: can be inlined into a
    /// FlatMap argument to bridge to a deeper group. (If it is also used as
    /// a value it is additionally evaluated in the upper layer; the rare
    /// double evaluation of one accessor is accepted.)
    Link,
    /// A member of a firing group: produced by the FlatMap stack.
    Unpacked,
    /// Everything else: stays above the stack.
    Above,
}

/// Plans the unpacking for one site, given as an MFP over the site's input.
/// Returns `None` if no group fires, in which case the caller must leave the
/// site untouched.
fn plan_site(mfp: &MapFilterProject) -> Option<SitePlan> {
    // Decline to rearrange sites containing literal errors, following the
    // optimizer convention of not moving those around. The MFP extraction
    // already stops at operators whose expressions *are* literal errors;
    // this additionally catches literal errors nested inside larger
    // expressions.
    if mfp.expressions.iter().any(contains_literal_err)
        || mfp.predicates.iter().any(|(_, p)| contains_literal_err(p))
    {
        return None;
    }

    let mut mfp = mfp.clone();
    mfp.memoize_expressions();
    let input_arity = mfp.input_arity;
    let exprs = &mfp.expressions;
    let n = exprs.len();

    // Pin everything transitively needed by a predicate.
    let mut pinned = vec![false; n];
    let mut worklist: Vec<usize> = Vec::new();
    for (_, predicate) in mfp.predicates.iter() {
        worklist.extend(
            predicate
                .support()
                .into_iter()
                .filter(|c| *c >= input_arity),
        );
    }
    while let Some(col) = worklist.pop() {
        let j = col - input_arity;
        if !pinned[j] {
            pinned[j] = true;
            worklist.extend(exprs[j].support().into_iter().filter(|c| *c >= input_arity));
        }
    }

    // Detect candidates and group the unpinned ones by base column.
    let candidates: Vec<Option<(usize, JsonbUnpackField)>> =
        exprs.iter().map(as_candidate).collect();
    let mut groups: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
    for (j, candidate) in candidates.iter().enumerate() {
        if let Some((base, _)) = candidate {
            if !pinned[j] {
                groups.entry(*base).or_default().push(j);
            }
        }
    }
    if groups.values().all(|g| g.len() < MIN_FIELDS_PER_UNPACK) {
        // Cheap exit before the classification pass.
        return None;
    }

    // Classify each column in a single forward pass (memoized expressions
    // only reference earlier columns, and a group's members all come after
    // its base column, so each group's firing decision is settled before it
    // is needed).
    let mut state: Vec<State> = vec![State::Below; input_arity];
    let mut fired: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
    for j in 0..n {
        let next = if pinned[j] {
            State::Below
        } else if let Some((base, _)) = &candidates[j] {
            let base_ok = matches!(
                state[*base],
                State::Below | State::BelowComputable | State::Link | State::Unpacked
            );
            if base_ok && groups[base].len() >= MIN_FIELDS_PER_UNPACK {
                fired.insert(*base, groups[base].clone());
                State::Unpacked
            } else if base_ok {
                State::Link
            } else {
                State::Above
            }
        } else if exprs[j]
            .support()
            .iter()
            .all(|c| matches!(state[*c], State::Below | State::BelowComputable))
        {
            State::BelowComputable
        } else {
            State::Above
        };
        state.push(next);
    }
    if fired.is_empty() {
        return None;
    }

    // Decide which below-computable expressions actually need to be
    // materialized below the stack: those (transitively) supporting a firing
    // group's base, including through inlined links.
    let mut placed = pinned.clone();
    let mut worklist: Vec<usize> = fired.keys().copied().collect();
    while let Some(col) = worklist.pop() {
        if col < input_arity {
            continue;
        }
        let j = col - input_arity;
        match state[col] {
            State::Below | State::BelowComputable => {
                if !placed[j] {
                    placed[j] = true;
                    worklist.extend(exprs[j].support());
                }
            }
            // The link is inlined into the FlatMap argument; what must be
            // materialized is whatever its own base needs.
            State::Link => worklist.push(candidates[j].as_ref().expect("link is a candidate").0),
            // Produced by an earlier FlatMap in the stack.
            State::Unpacked => {}
            State::Above => unreachable!("a firing base is never Above"),
        }
    }

    // Determine which placed expressions the layers above the lower MFP
    // demand: the direct support of the FlatMap arguments and of the upper
    // layer. Everything else placed (e.g. predicate support) is consumed
    // within the lower MFP and projected away below the stack.
    //
    // "The upper layer" is over-approximated as every expression not
    // available from below, whether or not the site's projection transitively
    // needs it; a slot demanded only by such dead expressions costs one
    // unused pass-through column below the stack. (Dead slots are rare here:
    // Demand and ProjectionPushdown ran earlier, and `memoize_expressions`
    // only creates slots for expressions it found inside live ones.)
    let mut demanded: BTreeSet<usize> = BTreeSet::new();
    fn collect_arg_support(
        col: usize,
        input_arity: usize,
        state: &[State],
        candidates: &[Option<(usize, JsonbUnpackField)>],
        demanded: &mut BTreeSet<usize>,
    ) {
        if col < input_arity {
            // Input columns are always passed through.
            return;
        }
        match state[col] {
            State::Below | State::BelowComputable => {
                demanded.insert(col);
            }
            // The link is inlined into the argument; its own base is what
            // the argument ends up referencing.
            State::Link => {
                let (base, _) = candidates[col - input_arity]
                    .as_ref()
                    .expect("link is a candidate");
                collect_arg_support(*base, input_arity, state, candidates, demanded);
            }
            // Produced by an earlier FlatMap in the stack.
            State::Unpacked => {}
            State::Above => unreachable!("a firing base is never Above"),
        }
    }
    for base in fired.keys() {
        collect_arg_support(*base, input_arity, &state, &candidates, &mut demanded);
    }
    for j in 0..n {
        let expressed_above = !placed[j] && state[input_arity + j] != State::Unpacked;
        if expressed_above {
            for c in exprs[j].support() {
                if c >= input_arity && placed[c - input_arity] {
                    demanded.insert(c);
                }
            }
        }
    }
    for p in mfp.projection.iter() {
        if *p >= input_arity && placed[p - input_arity] {
            demanded.insert(*p);
        }
    }

    // Build the lower MFP: input columns stay put, then the pinned
    // expressions (so that all predicates evaluate before any computed base
    // can error), then the remaining placed expressions; the projection
    // passes through the input columns and the demanded expressions.
    // `optimize` inlines single-use expressions (e.g. predicate support back
    // into the predicates) and drops anything unused.
    let lower_slots: Vec<usize> = (0..n)
        .filter(|j| placed[*j] && pinned[*j])
        .chain((0..n).filter(|j| placed[*j] && !pinned[*j]))
        .collect();
    let mut internal: BTreeMap<usize, usize> = (0..input_arity).map(|c| (c, c)).collect();
    for (rank, j) in lower_slots.iter().enumerate() {
        internal.insert(input_arity + j, input_arity + rank);
    }
    let lower_map: Vec<MirScalarExpr> = lower_slots
        .iter()
        .map(|j| {
            let mut e = exprs[*j].clone();
            e.permute_map(&internal);
            e
        })
        .collect();
    let lower_filter: Vec<MirScalarExpr> = mfp
        .predicates
        .iter()
        .map(|(_, p)| {
            let mut p = p.clone();
            p.permute_map(&internal);
            p
        })
        .collect();
    let demanded_slots: Vec<usize> = lower_slots
        .iter()
        .filter(|j| demanded.contains(&(input_arity + **j)))
        .copied()
        .collect();
    let mut lower_mfp = MapFilterProject::new(input_arity)
        .map(lower_map)
        .filter(lower_filter)
        .project(
            (0..input_arity).chain(demanded_slots.iter().map(|j| internal[&(input_arity + j)])),
        );
    lower_mfp.optimize();

    // The layout the FlatMap stack builds on: the lower MFP's output.
    let mut loc: BTreeMap<usize, usize> = (0..input_arity).map(|c| (c, c)).collect();
    for (rank, j) in demanded_slots.iter().enumerate() {
        loc.insert(input_arity + j, input_arity + rank);
    }

    // Build the FlatMap stack, in order of base column. Any column an
    // argument references is produced either below the stack or by an
    // earlier FlatMap (a group's base precedes its members, so a base that
    // is itself unpacked belongs to a group with a strictly smaller base).
    let mut flat_maps: Vec<(Vec<JsonbUnpackField>, MirScalarExpr)> = Vec::new();
    let mut next_col = input_arity + demanded_slots.len();
    for (base, members) in &fired {
        let arg = express_arg(*base, input_arity, &loc, &state, &candidates, exprs);
        let fields = members
            .iter()
            .map(|j| {
                candidates[*j]
                    .as_ref()
                    .expect("member is a candidate")
                    .1
                    .clone()
            })
            .collect::<Vec<_>>();
        for j in members {
            loc.insert(input_arity + j, next_col);
            next_col += 1;
        }
        flat_maps.push((fields, arg));
    }
    let post_arity = next_col;

    // Express the site's outputs over the post-stack layout. Expressions
    // not available from below get an entry in the upper map (in memoized
    // order, so references to other upper entries point backwards);
    // `optimize` then inlines pass-through column references and drops
    // undemanded entries.
    let remap_upper = |c: usize| -> usize {
        match loc.get(&c) {
            Some(l) => *l,
            None => post_arity + (c - input_arity),
        }
    };
    let upper_exprs: Vec<MirScalarExpr> = (0..n)
        .map(|j| match loc.get(&(input_arity + j)) {
            Some(l) => MirScalarExpr::column(*l),
            None => {
                let mut e = exprs[j].clone();
                e.visit_columns(|c| *c = remap_upper(*c));
                e
            }
        })
        .collect();
    let mut upper_mfp = MapFilterProject::new(post_arity)
        .map(upper_exprs)
        .project(mfp.projection.iter().map(|p| remap_upper(*p)));
    upper_mfp.optimize();

    Some(SitePlan {
        lower_mfp,
        flat_maps,
        upper_mfp,
    })
}

/// The expression to pass as the FlatMap argument for a group based on
/// column `col`: a column reference if the base is materialized below the
/// stack or unpacked by an earlier FlatMap, or the inlined accessor chain
/// for link columns.
fn express_arg(
    col: usize,
    input_arity: usize,
    loc: &BTreeMap<usize, usize>,
    state: &[State],
    candidates: &[Option<(usize, JsonbUnpackField)>],
    exprs: &[MirScalarExpr],
) -> MirScalarExpr {
    if let Some(l) = loc.get(&col) {
        return MirScalarExpr::column(*l);
    }
    debug_assert_eq!(state[col], State::Link);
    let j = col - input_arity;
    let (base, _) = candidates[j].as_ref().expect("link is a candidate");
    let mut e = exprs[j].clone();
    let MirScalarExpr::CallBinary { expr1, .. } = &mut e else {
        unreachable!("candidates are binary accessor calls")
    };
    **expr1 = express_arg(*base, input_arity, loc, state, candidates, exprs);
    e
}

/// Recognizes a memoized expression as an unpackable accessor:
/// `<column> (-> | ->> | #> | #>>) <non-null literal>`.
fn as_candidate(expr: &MirScalarExpr) -> Option<(usize, JsonbUnpackField)> {
    let MirScalarExpr::CallBinary { func, expr1, expr2 } = expr else {
        return None;
    };
    let stringify = match func {
        BinaryFunc::JsonbGetString(_)
        | BinaryFunc::JsonbGetInt64(_)
        | BinaryFunc::JsonbGetPath(_) => false,
        BinaryFunc::JsonbGetStringStringify(_)
        | BinaryFunc::JsonbGetInt64Stringify(_)
        | BinaryFunc::JsonbGetPathStringify(_) => true,
        _ => return None,
    };
    // After memoization the value side is a column (or a literal, which
    // `FoldConstants` owns).
    let MirScalarExpr::Column(base, _) = &**expr1 else {
        return None;
    };
    // Non-literal keys are out of scope; see the module documentation.
    let Some(Ok(datum)) = expr2.as_literal() else {
        return None;
    };
    let kind = match (func, datum) {
        (
            BinaryFunc::JsonbGetString(_) | BinaryFunc::JsonbGetStringStringify(_),
            Datum::String(key),
        ) => JsonbUnpackFieldKind::Key(key.to_owned()),
        (BinaryFunc::JsonbGetInt64(_) | BinaryFunc::JsonbGetInt64Stringify(_), Datum::Int64(i)) => {
            JsonbUnpackFieldKind::Index(i)
        }
        (BinaryFunc::JsonbGetPath(_) | BinaryFunc::JsonbGetPathStringify(_), Datum::Array(arr)) => {
            let mut path = Vec::new();
            for element in arr.elements().iter() {
                match element {
                    Datum::String(s) => path.push(s.to_owned()),
                    // A null path element makes the whole accessor NULL; not
                    // worth special-casing.
                    _ => return None,
                }
            }
            if path.is_empty() {
                // `#> '{}'` is the identity; degenerate, skip.
                return None;
            }
            JsonbUnpackFieldKind::Path(path)
        }
        // NULL keys (and non-matching literal shapes) are constant-foldable;
        // not candidates.
        _ => return None,
    };
    Some((*base, JsonbUnpackField { kind, stringify }))
}

/// Converts an MFP without predicates into one standalone expression per
/// projection output, inlining all map expressions.
fn standalone_exprs(mfp: &MapFilterProject) -> Vec<MirScalarExpr> {
    assert!(mfp.predicates.is_empty());
    fn col_expr(c: usize, mfp: &MapFilterProject) -> MirScalarExpr {
        if c < mfp.input_arity {
            MirScalarExpr::column(c)
        } else {
            let mut e = mfp.expressions[c - mfp.input_arity].clone();
            e.visit_pre_mut(|node| {
                if let MirScalarExpr::Column(c, _) = node {
                    if *c >= mfp.input_arity {
                        *node = col_expr(*c, mfp);
                    }
                }
            });
            e
        }
    }
    mfp.projection.iter().map(|p| col_expr(*p, mfp)).collect()
}

/// True iff the expression contains a literal error anywhere.
fn contains_literal_err(expr: &MirScalarExpr) -> bool {
    let mut found = false;
    expr.visit_pre(|e| {
        if e.is_literal_err() {
            found = true;
        }
    });
    found
}
