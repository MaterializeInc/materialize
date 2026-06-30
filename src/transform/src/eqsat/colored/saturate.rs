// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! The colored rule driver: run the tagged ("colored") rewrite rules over a
//! [`ColoredView`] per color to a bounded fixpoint, applying each conclusion via
//! `add_colored` and closing congruence with [`ColoredEGraph::close`].
//!
//! This is the colored mirror of the base [`EGraph::saturate`] two-phase loop
//! (`eqsat::egraph::saturate`): Phase 1 enumerates matches over an immutable
//! per-round snapshot (`ColoredView`), Phase 2 instantiates right-hand sides
//! (mutating the color's delta overlay) and records the resulting equalities,
//! which `close` then applies and propagates by congruence. Unlike the base
//! loop it never touches the shared base — every conclusion stays in the color.
//!
//! Wired into the engine at SP4d Task 11 (four extraction sites in `engine.rs`,
//! gated on `engine::COLORED_SATURATION`).

use crate::eqsat::colored::view::ColoredView;
use crate::eqsat::colored::{ColorId, Id};
use crate::eqsat::colored_derive::ColoredLayer;
use crate::eqsat::egraph::{Analyses, COLORED_MAX_ITERS, EGraph, MATCH_LIMIT, MAX_ENODES, Sym};
use crate::eqsat::rules::{colored_apply, colored_find_all};

/// Run the colored rewrite rules over `layer` to a bounded fixpoint, per color,
/// returning the total number of rounds executed across all colors.
///
/// For each color (flat declaration order, which is parent-first because colors
/// are created parent-first), repeats up to [`COLORED_MAX_ITERS`] rounds. Each
/// round mirrors the base two-phase structure:
///
/// * **Phase 1** builds a fresh [`ColoredView`] snapshot and collects every
///   colored-rule match (`colored_find_all`, `&view`).
/// * **Phase 2** instantiates each match's right-hand side (`colored_apply`,
///   `&mut view`), recording `(new_id, root)` equalities, and bails out of the
///   pass once the color's delta-node count exceeds [`MAX_ENODES`].
///
/// After the view is dropped, [`ColoredEGraph::close`] applies the equalities and
/// closes congruence in the color. A round that merges nothing ends the loop for
/// that color. The caller-owned `delta_escalar` cache on the layer persists
/// across rounds and colors so colored-delta scalar ids stay resolvable.
pub(crate) fn colored_saturate<'b>(layer: &mut ColoredLayer<'b>, base: &'b EGraph) -> usize {
    // Valid because every colored-tagged rule is Filter-rooted; revisit if a
    // non-Filter rule is tagged colored.
    if !base.has_rel_sym(Sym::Filter) {
        return 0;
    }

    let mut total = 0;
    for ci in 0..layer.ceg.num_colors() {
        let color = ColorId(ci);
        for _ in 0..COLORED_MAX_ITERS {
            let mut equalities: Vec<(Id, Id)> = Vec::new();
            {
                // Split-borrow the layer so the view holds `&mut ceg` and the
                // caller-owned `&mut delta_escalar` simultaneously. Both borrows
                // end when `view` is dropped at the close of this block, freeing
                // `layer.ceg` for `close` below.
                let ColoredLayer {
                    ceg, delta_escalar, ..
                } = &mut *layer;
                let mut view = ColoredView::new(ceg, color, base, delta_escalar);

                // Phase 1 (read-only): collect all colored-rule matches over the
                // snapshot. Colored rules read no analyses, so an empty
                // `Analyses` suffices (their analysis-gated conditions can never
                // fire — enforced at build time, Task 4).
                let matches = colored_find_all(&view, &Analyses::default(), MATCH_LIMIT + 1);

                // Phase 2 (mutate): instantiate right-hand sides into the color's
                // delta overlay and record the resulting equalities. The
                // delta-node budget is rechecked per application because one pass
                // can add many colored nodes; stopping early is sound (fewer
                // conclusions, never an incorrect one).
                for (idx, b) in matches {
                    if let Ok(new_id) = colored_apply(idx, &mut view, &b) {
                        equalities.push((new_id, b.root));
                    }
                    if view.ceg.colored_node_count(color) > MAX_ENODES {
                        break;
                    }
                }
            }

            let metrics = layer.ceg.close(color, &equalities);
            total += 1;
            let changed = metrics.applied_equalities > 0 || metrics.induced_merges > 0;
            if !changed {
                break;
            }
        }
    }
    total
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use mz_expr::{BinaryFunc, MirScalarExpr, func};
    use mz_repr::{Datum, ReprScalarType};

    use crate::eqsat::colored::ColoredEGraph;
    use crate::eqsat::colored_derive::ColoredLayer;
    use crate::eqsat::egraph::{CNode, EGraph, ENode};
    use crate::eqsat::ir::EScalar;

    use super::colored_saturate;

    /// Early-out #1 guard: `colored_saturate` must return 0 rounds when the base
    /// e-graph contains no `Filter` e-node. Every colored-tagged rule has a
    /// Filter-rooted left-hand side, so no rule can ever match and running the
    /// per-color loop is provably wasted work.
    ///
    /// Without the early-out the current code runs one (empty) round per color
    /// before the `!changed` break fires, returning 1; the test catches that.
    #[mz_ore::test]
    fn colored_saturate_no_rounds_when_no_filter() {
        // A graph with NO Filter e-node — only a Constant leaf.
        let mut eg = EGraph::new();
        let _leaf = eg.add(CNode::Rel(ENode::Constant {
            card: 1,
            arity: 1,
            col_types: None,
        }));
        eg.rebuild();

        // One color: there is nothing for any colored rule to match.
        let mut ceg = ColoredEGraph::new(&eg);
        let _c = ceg.new_color(None);
        let mut layer = ColoredLayer {
            ceg,
            color_of: HashMap::new(),
            empty_classes: HashSet::new(),
            delta_escalar: HashMap::new(),
        };

        let n = colored_saturate(&mut layer, &eg);
        assert_eq!(
            n, 0,
            "colored_saturate must return 0 rounds when the base graph has no \
             Filter e-node (all colored rules are Filter-rooted, so none can match)"
        );
    }

    /// Under a color asserting the Filter predicate `(#0 = #1)` is `true`, the
    /// colored rule `drop_true_filter` fires within the color, producing the
    /// conclusion `Filter[p](leaf) ≅ leaf`. `colored_saturate` must run at least
    /// one round and leave the Filter's class colored-merged with its input.
    ///
    /// (The brief frames this as a "context that makes a predicate true": the
    /// cleanest deterministic such context is asserting the predicate's scalar
    /// class congruent to the literal-`true` class, so the colored-canonical
    /// predicate resolves to `true` and `all_true(p)` holds.)
    #[mz_ore::test]
    fn colored_saturate_simplifies_under_context() {
        let mut eg = EGraph::new();
        let leaf = eg.add(CNode::Rel(ENode::Constant {
            card: 1,
            arity: 2,
            col_types: None,
        }));
        // Predicate `#0 = #1` (lit `None`: not a constant on its own).
        let pred = eg.intern_scalar(&EScalar::plain(
            MirScalarExpr::column(0)
                .call_binary(MirScalarExpr::column(1), BinaryFunc::Eq(func::Eq)),
        ));
        // The literal `true` scalar class.
        let tru = eg.intern_scalar(&EScalar::plain(MirScalarExpr::literal_ok(
            Datum::True,
            ReprScalarType::Bool,
        )));
        let filt = eg.add(CNode::Rel(ENode::Filter {
            predicates: vec![pred],
            input: leaf,
        }));
        eg.rebuild();

        // One flat color asserting `predicate ≅ true`. Union `(true, pred)` so
        // the literal-`true` class wins the (equal-size) union and is the
        // colored representative, hence the canonical predicate's `lit` is
        // `Some(true)` and `all_true(p)` holds.
        let mut ceg = ColoredEGraph::new(&eg);
        let color = ceg.new_color(None);
        ceg.union(color, eg.find(tru), eg.find(pred));
        let mut color_of = HashMap::new();
        color_of.insert(eg.find(filt), color);
        let mut layer = ColoredLayer {
            ceg,
            color_of,
            empty_classes: HashSet::new(),
            delta_escalar: HashMap::new(),
        };

        let n = colored_saturate(&mut layer, &eg);
        assert!(n > 0, "colored_saturate ran at least one round, got {n}");

        // Under the color, the Filter's class colored-merged with its input
        // (`drop_true_filter` concluded `Filter[true](leaf) ≅ leaf`).
        let members = layer.ceg.colored_class_members(color, eg.find(filt));
        assert!(
            members.contains(&eg.find(leaf)),
            "filter class must colored-merge with its input leaf under the context, got {members:?}",
        );
    }
}
