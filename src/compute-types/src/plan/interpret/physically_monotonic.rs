// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Implementation of [crate::plan::interpret::Interpreter] for inference
//! of physical monotonicity in single-time dataflows.

use std::cmp::Reverse;
use std::collections::BTreeSet;

use differential_dataflow::lattice::Lattice;
use mz_expr::{EvalError, Id, MfpPlan, SafeMfpPlan, TableFunc};
use mz_repr::{Diff, GlobalId, Row, Timestamp};
use timely::PartialOrder;

use crate::plan::interpret::{BoundedLattice, Context, Interpreter};
use crate::plan::join::JoinPlan;
use crate::plan::reduce::{KeyValPlan, ReducePlan};
use crate::plan::scalar::LirScalarExpr;
use crate::plan::threshold::ThresholdPlan;
use crate::plan::top_k::TopKPlan;
use crate::plan::{AvailableCollections, GetPlan};

/// Represents a boolean physical monotonicity property, where the bottom value
/// is true (i.e., physically monotonic) and the top value is false (i.e. not
/// physically monotonic).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct PhysicallyMonotonic(pub bool);

impl BoundedLattice for PhysicallyMonotonic {
    fn top() -> Self {
        PhysicallyMonotonic(false)
    }

    fn bottom() -> Self {
        PhysicallyMonotonic(true)
    }
}

impl Lattice for PhysicallyMonotonic {
    fn join(&self, other: &Self) -> Self {
        PhysicallyMonotonic(self.0 && other.0)
    }

    fn meet(&self, other: &Self) -> Self {
        PhysicallyMonotonic(self.0 || other.0)
    }
}

impl PartialOrder for PhysicallyMonotonic {
    fn less_equal(&self, other: &Self) -> bool {
        // We employ `Reverse` ordering for `bool` here to be consistent with
        // the choice of `top()` being false and `bottom()` being true.
        Reverse::<bool>(self.0) <= Reverse::<bool>(other.0)
    }
}

/// Provides a concrete implementation of an interpreter that determines if
/// the output of `LirRelationExpr` expressions is physically monotonic in a single-time
/// dataflow, potentially taking into account judgments about its inputs. We
/// note that in a single-time dataflow, expressions in non-recursive contexts
/// (i.e., outside of `LetRec` values) process streams that are at a minimum
/// logically monotonic, i.e., may contain retractions but would cease to do
/// so if consolidated. Detecting physical monotonicity, i.e., the absence
/// of retractions in a stream, enables us to disable forced consolidation
/// whenever possible.
#[derive(Debug)]
pub struct SingleTimeMonotonic<'a> {
    monotonic_ids: &'a BTreeSet<GlobalId>,
}

impl<'a> SingleTimeMonotonic<'a> {
    /// Instantiates an interpreter for single-time physical monotonicity
    /// analysis.
    pub fn new(monotonic_ids: &'a BTreeSet<GlobalId>) -> Self {
        SingleTimeMonotonic { monotonic_ids }
    }
}

impl Interpreter for SingleTimeMonotonic<'_> {
    type Domain = PhysicallyMonotonic;

    fn constant(
        &self,
        _ctx: &Context<Self::Domain>,
        rows: &Result<Vec<(Row, Timestamp, Diff)>, EvalError>,
    ) -> Self::Domain {
        // A constant is physically monotonic iff the constant is an `EvalError`
        // or all its rows have `Diff` values greater than zero.
        PhysicallyMonotonic(rows.as_ref().map_or(true, |rows| {
            rows.iter().all(|(_, _, diff)| *diff > Diff::ZERO)
        }))
    }

    fn get(
        &self,
        ctx: &Context<Self::Domain>,
        id: &Id,
        _keys: &AvailableCollections,
        _plan: &GetPlan,
    ) -> Self::Domain {
        // A get operator yields physically monotonic output iff the corresponding
        // `LirRelationExpr::Get` is on a local or global ID that is known to provide physically
        // monotonic input. The way this becomes know is through the interpreter itself
        // for non-recursive local IDs or through configuration for the global IDs of
        // monotonic sources and indexes. Recursive local IDs are always assumed to
        // break physical monotonicity.
        // TODO(vmarcos): Consider in the future if we can ascertain whether the
        // restrictions on recursive local IDs can be relaxed to take into account only
        // the interpreter judgement directly.
        PhysicallyMonotonic(match id {
            Id::Local(id) => ctx
                .bindings
                .get(id)
                .map_or(false, |entry| !entry.is_rec && entry.value.0),
            Id::Global(id) => self.monotonic_ids.contains(id),
        })
    }

    fn mfp(
        &self,
        _ctx: &Context<Self::Domain>,
        input: Self::Domain,
        mfp: &MfpPlan<LirScalarExpr>,
        _input_key_val: &Option<(Vec<LirScalarExpr>, Option<Row>)>,
    ) -> Self::Domain {
        // In a single-time context, we propagate the monotonicity status of the
        // input, conservatively treating an MFP with temporal predicates as
        // breaking physical monotonicity. Note that an MFP cannot itself produce
        // negative diffs (map/filter/project never negate), so this is stricter
        // than strictly necessary for a single-time dataflow: temporal predicates
        // (`mz_now()`) only introduce retractions *across* timestamps, and a
        // one-shot dataflow runs at a single time. We keep the check anyway as
        // defense-in-depth and to mirror the judgment made for
        // `MirRelationExpr::Filter` in the MIR monotonicity analysis.
        PhysicallyMonotonic(input.0 && !mfp.has_temporal_bounds())
    }

    fn flat_map(
        &self,
        _ctx: &Context<Self::Domain>,
        _input_key: &Option<Vec<LirScalarExpr>>,
        input: Self::Domain,
        _exprs: &Vec<LirScalarExpr>,
        func: &TableFunc,
        mfp: &MfpPlan<LirScalarExpr>,
    ) -> Self::Domain {
        // In a single-time context, we propagate the monotonicity status of the
        // input, but only if the table function preserves the append-only
        // property of its input. A table function such as `repeat_row` can emit
        // negative diffs and so does not preserve monotonicity; this is the only
        // substantive check here. The post-MFP temporal-predicate check is, as in
        // `mfp()` above, conservative defense-in-depth: temporal predicates only
        // introduce retractions across timestamps and a one-shot dataflow runs at
        // a single time. Together this mirrors the judgment made for
        // `MirRelationExpr::FlatMap` (combined with `MirRelationExpr::Filter`) in
        // the MIR monotonicity analysis.
        PhysicallyMonotonic(input.0 && func.preserves_monotonicity() && !mfp.has_temporal_bounds())
    }

    fn join(
        &self,
        _ctx: &Context<Self::Domain>,
        inputs: Vec<Self::Domain>,
        _plan: &JoinPlan,
    ) -> Self::Domain {
        // When we see a join, we must consider that the inputs could have
        // been `LirRelationExpr::Get`s on arrangements. These are not in general safe
        // wrt. producing physically monotonic data. So here, we conservatively
        // judge that output of a join to be physically monotonic iff all
        // inputs are physically monotonic.
        PhysicallyMonotonic(inputs.iter().all(|monotonic| monotonic.0))
    }

    fn reduce(
        &self,
        ctx: &Context<Self::Domain>,
        _input_key: &Option<Vec<LirScalarExpr>>,
        _input: Self::Domain,
        _key_val_plan: &KeyValPlan,
        _plan: &ReducePlan,
        _mfp_after: &SafeMfpPlan<LirScalarExpr>,
    ) -> Self::Domain {
        // In a recursive context, reduce will advance across timestamps
        // and may need to retract. Outside of a recursive context, the
        // fact that the dataflow is single-time implies no retraction
        // is emitted out of reduce. This makes the output be physically
        // monotonic, regardless of the input judgment. All `ReducePlan`
        // variants behave the same in this respect.
        PhysicallyMonotonic(!ctx.is_rec)
    }

    fn top_k(
        &self,
        ctx: &Context<Self::Domain>,
        _input: Self::Domain,
        _top_k_plan: &TopKPlan,
    ) -> Self::Domain {
        // Top-k behaves like a reduction, producing physically monotonic
        // output when exposed to a single time (i.e., when the context is
        // non-recursive). Note that even a monotonic top-k will consolidate
        // if necessary to ensure this property.
        PhysicallyMonotonic(!ctx.is_rec)
    }

    fn negate(&self, _ctx: &Context<Self::Domain>, _input: Self::Domain) -> Self::Domain {
        // Negation produces retractions, so it breaks physical monotonicity.
        PhysicallyMonotonic(false)
    }

    fn threshold(
        &self,
        ctx: &Context<Self::Domain>,
        _input: Self::Domain,
        _threshold_plan: &ThresholdPlan,
    ) -> Self::Domain {
        // Thresholding is a special kind of reduction, so the judgment
        // here is the same as for reduce.
        PhysicallyMonotonic(!ctx.is_rec)
    }

    fn union(
        &self,
        _ctx: &Context<Self::Domain>,
        inputs: Vec<Self::Domain>,
        _consolidate_output: bool,
    ) -> Self::Domain {
        // Union just concatenates the inputs, so is physically monotonic iff
        // all inputs are physically monotonic.
        // (Even when we do consolidation, we can't be certain that a negative diff from an input
        // is actually cancelled out. For example, Union outputs negative diffs when it's part of
        // the EXCEPT pattern.)
        PhysicallyMonotonic(inputs.iter().all(|monotonic| monotonic.0))
    }

    fn arrange_by(
        &self,
        _ctx: &Context<Self::Domain>,
        input: Self::Domain,
        _forms: &AvailableCollections,
        _input_key: &Option<Vec<LirScalarExpr>>,
        _input_mfp: &MfpPlan<LirScalarExpr>,
    ) -> Self::Domain {
        // `LirRelationExpr::ArrangeBy` is better thought of as `ensure_collections`, i.e., it
        // makes sure that the requested `forms` are present and builds them only
        // if not already available. Many `forms` may be requested, as the downstream
        // consumers of this operator may be many different ones (as we support plan graphs,
        // not only trees). The `forms` include arrangements, but also just the collection
        // in `raw` form. So for example, if the input is arranged, then `ArrangeBy` could
        // be used to request a collection instead. `ArrangeBy` will only build an arrangement
        // from scratch when the input is not already arranged in a requested `form`. In our
        // physical monotonicity analysis, we presently cannot assert whether only arrangements
        // that `ArrangeBy` built will be used by downstream consumers, or if other `forms` that
        // do not preserve physical monotonicity would be accessed instead. So we conservatively
        // return the physical monotonicity judgment made for the input.
        // TODO(vmarcos): Consider in the future enriching the analysis to track physical
        // monotonicity not by the output of an operator, but by `forms` made available for each
        // collection. With this information, we could eventually make more refined judgements
        // at the points of use.
        input
    }
}

#[cfg(test)]
mod tests {
    use mz_expr::{MapFilterProject, MirScalarExpr, UnmaterializableFunc};
    use mz_repr::Datum;

    use crate::plan::scalar::mfp_mir_to_lir_plan;

    use super::*;

    /// A `MapFilterProject` over `arity` columns that contains a temporal
    /// predicate (i.e., a predicate referencing `mz_now()`).
    fn temporal_mfp(arity: usize) -> MapFilterProject {
        MapFilterProject::new(arity).filter([MirScalarExpr::call_binary(
            MirScalarExpr::literal(Ok(Datum::UInt64(0)), mz_repr::ReprScalarType::UInt64),
            MirScalarExpr::CallUnmaterializable(UnmaterializableFunc::MzNow),
            mz_expr::func::Lte,
        )])
    }

    #[mz_ore::test]
    fn flat_map_preserves_monotonicity_for_monotonicity_preserving_func() {
        let monotonic_ids = BTreeSet::new();
        let interpreter = SingleTimeMonotonic::new(&monotonic_ids);
        let ctx = Context::default();
        let mfp = MapFilterProject::new(1).into_plan().unwrap();

        // A monotonicity-preserving table function (e.g., `generate_series`)
        // propagates the input's monotonicity.
        let result = interpreter.flat_map(
            &ctx,
            &None,
            PhysicallyMonotonic(true),
            &vec![],
            &TableFunc::GenerateSeriesInt64,
            &mfp,
        );
        assert_eq!(result, PhysicallyMonotonic(true));

        // A non-monotonic input stays non-monotonic.
        let result = interpreter.flat_map(
            &ctx,
            &None,
            PhysicallyMonotonic(false),
            &vec![],
            &TableFunc::GenerateSeriesInt64,
            &mfp,
        );
        assert_eq!(result, PhysicallyMonotonic(false));
    }

    #[mz_ore::test]
    fn flat_map_breaks_monotonicity_for_non_monotonicity_preserving_func() {
        let monotonic_ids = BTreeSet::new();
        let interpreter = SingleTimeMonotonic::new(&monotonic_ids);
        let ctx = Context::default();
        let mfp = MapFilterProject::new(1).into_plan().unwrap();

        // `repeat_row` does not preserve monotonicity (it can emit retractions),
        // so even a physically monotonic input yields a non-monotonic output.
        // This is the bug from CLU-68: the interpreter used to blindly propagate
        // the input's monotonicity here.
        let result = interpreter.flat_map(
            &ctx,
            &None,
            PhysicallyMonotonic(true),
            &vec![],
            &TableFunc::RepeatRow,
            &mfp,
        );
        assert_eq!(result, PhysicallyMonotonic(false));
    }

    #[mz_ore::test]
    fn flat_map_breaks_monotonicity_for_temporal_mfp() {
        let monotonic_ids = BTreeSet::new();
        let interpreter = SingleTimeMonotonic::new(&monotonic_ids);
        let ctx = Context::default();
        let mfp = mfp_mir_to_lir_plan(temporal_mfp(1));

        // Even with a monotonicity-preserving table function and a monotonic
        // input, a temporal predicate in the after-MFP breaks monotonicity, as
        // it can result in the future removal of records.
        let result = interpreter.flat_map(
            &ctx,
            &None,
            PhysicallyMonotonic(true),
            &vec![],
            &TableFunc::GenerateSeriesInt64,
            &mfp,
        );
        assert_eq!(result, PhysicallyMonotonic(false));
    }

    #[mz_ore::test]
    fn mfp_propagates_monotonicity_without_temporal_predicates() {
        let monotonic_ids = BTreeSet::new();
        let interpreter = SingleTimeMonotonic::new(&monotonic_ids);
        let ctx = Context::default();
        let mfp = mfp_mir_to_lir_plan(MapFilterProject::new(1));

        // Without temporal predicates, the MFP just propagates its input's
        // monotonicity.
        let result = interpreter.mfp(&ctx, PhysicallyMonotonic(true), &mfp, &None);
        assert_eq!(result, PhysicallyMonotonic(true));

        let result = interpreter.mfp(&ctx, PhysicallyMonotonic(false), &mfp, &None);
        assert_eq!(result, PhysicallyMonotonic(false));
    }

    #[mz_ore::test]
    fn mfp_breaks_monotonicity_for_temporal_predicates() {
        let monotonic_ids = BTreeSet::new();
        let interpreter = SingleTimeMonotonic::new(&monotonic_ids);
        let ctx = Context::default();
        let mfp = mfp_mir_to_lir_plan(temporal_mfp(1));

        // A temporal predicate breaks monotonicity even for a monotonic input.
        let result = interpreter.mfp(&ctx, PhysicallyMonotonic(true), &mfp, &None);
        assert_eq!(result, PhysicallyMonotonic(false));
    }
}
