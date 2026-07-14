// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Rendering of the fused `SetDifference` operator.
//!
//! `SetDifference { base, subtract, key }` computes, per `key`, the thresholded
//! net multiplicity `net = sum_v count_base(key, v) - sum_v count_subtract(key, v)`,
//! emitting `(key, (), net)` whenever `net > 0`. This reproduces
//! `Threshold(Union(Negate(subtract), base))` exactly: the anti-side `Threshold`
//! keys on the entire difference row, so per-key accounting equals per-row
//! accounting. The operator ignores value contents (it only sums diffs), so the
//! two input arrangements may carry different `thinning`.
//!
//! The point of the fused operator is to consume the two already-existing input
//! arrangements directly and produce the one output arrangement, without the
//! intermediate `ArrangeBy` (trace T) that the reconstructed
//! `Threshold(ArrangeBy(Union(..)))` dataflow would build.
//!
//! The two-input reduce machinery lives in [`co_reduce2`], a reduce over two
//! co-arranged inputs. This module only supplies the per-key threshold closure
//! [`set_difference_threshold`] and demultiplexes the four `Local`/`Trace`
//! flavor combinations of the two input arrangements onto it by monomorphization.
//!
//! [`co_reduce2`]: mz_timely_util::co_reduce::co_reduce2

use mz_compute_types::plan::scalar::{LirScalarExpr, mfp_mir_to_lir_plan};
use mz_compute_types::plan::{ArrangementStrategy, AvailableCollections};
use mz_expr::MapFilterProject;
use mz_repr::{Diff, Row};
use mz_timely_util::co_reduce::co_reduce2;
use mz_timely_util::columnation::ColumnationChunker;

use crate::extensions::arrange::{ArrangementSize, KeyCollection, MzArrange};
use crate::render::context::{ArrangementFlavor, CollectionBundle, Context};
use crate::render::{MaybeBucketByTime, RenderTimestamp};
use crate::typedefs::{ErrBatcher, ErrBuilder, RowRowAgent, RowRowEnter, RowRowSpine};
use mz_row_spine::RowRowBuilder;

/// One input arm, read through its existing arrangement keyed by the arm's own key.
///
/// The recognizer rewrites each arm so its rendered bundle exposes an existing arrangement
/// directly (`Get::PassArrangements` or an `ArrangeBy`), stripping any projection that would
/// otherwise render the arm as a raw collection. So the arm's arrangement lookup hits and no
/// per-arm re-arrangement is built, which is what lets the fusion eliminate the shared
/// intermediate arrangement (trace T) over the union without reintroducing one per arm.
enum ArmArrangement<'scope, T: RenderTimestamp> {
    Local(differential_dataflow::operators::arrange::Arranged<'scope, RowRowAgent<T, Diff>>),
    Trace(
        differential_dataflow::operators::arrange::Arranged<
            'scope,
            RowRowEnter<mz_repr::Timestamp, Diff, T>,
        >,
    ),
}

impl<'scope, T: RenderTimestamp + MaybeBucketByTime> Context<'scope, T> {
    /// Renders a `SetDifference` node into a single output arrangement.
    ///
    /// `base` must be available arranged on `base_key` and `subtract` on
    /// `subtract_key`, which the recognizer guarantees. Those two keys carry the
    /// same key datums as the output key (`ensure_arrangement.0`), so reading each
    /// arm through its own arrangement produces aligned keys. The output is a
    /// `Local` arrangement keyed by the output key with empty (`thinning=()`)
    /// values, matching the `Threshold` output this node replaces.
    pub(crate) fn render_set_difference(
        &self,
        base: CollectionBundle<'scope, T>,
        subtract: CollectionBundle<'scope, T>,
        base_key: Vec<LirScalarExpr>,
        subtract_key: Vec<LirScalarExpr>,
        ensure_arrangement: (Vec<LirScalarExpr>, Vec<usize>, Vec<usize>),
    ) -> CollectionBundle<'scope, T> {
        let key = ensure_arrangement.0.clone();

        // Errors from both inputs are converted to collections, concatenated, and
        // arranged once, mirroring the reduce error pattern in `render/reduce.rs`.
        let mut err_collections = Vec::with_capacity(2);

        // Read each arm through its own existing arrangement (`base_key` /
        // `subtract_key`). Their key datums align with the output key, so the
        // per-key net accounting co-iterates correctly across arms.
        let base_arm =
            self.arm_arrangement(base, &base_key, &ensure_arrangement, &mut err_collections);
        let sub_arm = self.arm_arrangement(
            subtract,
            &subtract_key,
            &ensure_arrangement,
            &mut err_collections,
        );

        // Demultiplex the four Local/Trace combinations onto `co_reduce2`, which is
        // generic over the two trace types. Each arm monomorphizes the operator for
        // its flavor pair. The output trace, builder, and value/diff types are pinned
        // to the empty-valued `RowRowSpine` output; the input trace types infer from
        // the arguments. `co_reduce2` does not log arrangement size, so the caller owns
        // that here.
        let oks = match (base_arm, sub_arm) {
            (ArmArrangement::Local(base_oks), ArmArrangement::Local(sub_oks)) => {
                co_reduce2::<
                    T,
                    RowRowAgent<T, Diff>,
                    RowRowAgent<T, Diff>,
                    Row,
                    Row,
                    Row,
                    Diff,
                    RowRowBuilder<T, Diff>,
                    RowRowSpine<T, Diff>,
                    _,
                >(base_oks, sub_oks, "SetDifference", set_difference_threshold)
                .log_arrangement_size()
            }
            (ArmArrangement::Local(base_oks), ArmArrangement::Trace(sub_oks)) => {
                co_reduce2::<
                    T,
                    RowRowAgent<T, Diff>,
                    RowRowEnter<mz_repr::Timestamp, Diff, T>,
                    Row,
                    Row,
                    Row,
                    Diff,
                    RowRowBuilder<T, Diff>,
                    RowRowSpine<T, Diff>,
                    _,
                >(base_oks, sub_oks, "SetDifference", set_difference_threshold)
                .log_arrangement_size()
            }
            (ArmArrangement::Trace(base_oks), ArmArrangement::Local(sub_oks)) => {
                co_reduce2::<
                    T,
                    RowRowEnter<mz_repr::Timestamp, Diff, T>,
                    RowRowAgent<T, Diff>,
                    Row,
                    Row,
                    Row,
                    Diff,
                    RowRowBuilder<T, Diff>,
                    RowRowSpine<T, Diff>,
                    _,
                >(base_oks, sub_oks, "SetDifference", set_difference_threshold)
                .log_arrangement_size()
            }
            (ArmArrangement::Trace(base_oks), ArmArrangement::Trace(sub_oks)) => {
                co_reduce2::<
                    T,
                    RowRowEnter<mz_repr::Timestamp, Diff, T>,
                    RowRowEnter<mz_repr::Timestamp, Diff, T>,
                    Row,
                    Row,
                    Row,
                    Diff,
                    RowRowBuilder<T, Diff>,
                    RowRowSpine<T, Diff>,
                    _,
                >(base_oks, sub_oks, "SetDifference", set_difference_threshold)
                .log_arrangement_size()
            }
        };

        let errs = differential_dataflow::collection::concatenate(self.scope, err_collections);
        let errs: KeyCollection<_, _, _> = errs.into();
        let errs = errs.mz_arrange::<ColumnationChunker<_>, ErrBatcher<_, _>, ErrBuilder<_, _>, _>(
            "Arrange SetDifference err",
        );

        CollectionBundle::from_expressions(key, ArrangementFlavor::Local(oks, errs))
    }

    /// Reads one input arm through its existing arrangement keyed by `arm_key`,
    /// pushing the arm's error collection into `errs`.
    ///
    /// The recognizer guarantees each recognized arm exposes an arrangement on its
    /// own key, so the `arm.arrangement(arm_key)` lookup hits. The `None` branch is
    /// a defensive fallback: it arranges the arm on the output `ensure_arrangement`
    /// once, mirroring the `ArrangeBy` the lowering would otherwise insert. The
    /// arrangement value is empty (`thinning=()`), which the operator ignores.
    fn arm_arrangement(
        &self,
        arm: CollectionBundle<'scope, T>,
        arm_key: &[LirScalarExpr],
        ensure_arrangement: &(Vec<LirScalarExpr>, Vec<usize>, Vec<usize>),
        errs: &mut Vec<
            differential_dataflow::VecCollection<
                'scope,
                T,
                crate::render::errors::DataflowErrorSer,
                Diff,
            >,
        >,
    ) -> ArmArrangement<'scope, T> {
        match arm.arrangement(arm_key) {
            Some(ArrangementFlavor::Local(oks, arm_errs)) => {
                errs.push(arm_errs.as_collection(|k, _v| k.clone()));
                ArmArrangement::Local(oks)
            }
            Some(ArrangementFlavor::Trace(_, oks, arm_errs)) => {
                errs.push(arm_errs.as_collection(|k, _v| k.clone()));
                ArmArrangement::Trace(oks)
            }
            None => {
                let arity = ensure_arrangement.0.len();
                let arranged = arm.ensure_collections(
                    AvailableCollections::new_arranged(vec![ensure_arrangement.clone()]),
                    None,
                    mfp_mir_to_lir_plan(MapFilterProject::new(arity)),
                    self.as_of_frontier.clone(),
                    self.until.clone(),
                    &self.config_set,
                    ArrangementStrategy::Direct,
                );
                match arranged
                    .arrangement(&ensure_arrangement.0)
                    .expect("arrangement just built on the output key")
                {
                    ArrangementFlavor::Local(oks, arm_errs) => {
                        errs.push(arm_errs.as_collection(|k, _v| k.clone()));
                        ArmArrangement::Local(oks)
                    }
                    ArrangementFlavor::Trace(..) => {
                        unreachable!("a freshly built arrangement is always `Local`")
                    }
                }
            }
        }
    }
}

/// Per-key threshold: keep the positive residual of base minus subtract on the
/// empty output value.
///
/// `inputs[0]` is `base`, `inputs[1]` is `subtract`. Both feed as `(value, diff)`
/// multisets, but the set-difference key ignores value contents, so it sums diffs
/// per input. The positive net is emitted on the empty row, reproducing the
/// `Threshold(Union(Negate(subtract), base))` this node replaces.
fn set_difference_threshold(_key: &Row, inputs: &[&[(Row, Diff)]], out: &mut Vec<(Row, Diff)>) {
    let sum = |s: &[(Row, Diff)]| s.iter().map(|(_v, d)| *d).sum::<Diff>();
    let net = sum(inputs[0]) - sum(inputs[1]);
    if net.is_positive() {
        out.push((Row::default(), net));
    }
}
