// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Integration test: the physical eqsat pass commits a literal filter over an
//! indexed `Get` to an `IndexedFilter` join.
//!
//! This runs `PhysicalEqSatTransform` in isolation, without the standalone
//! `LiteralConstraints` pass that also produces `IndexedFilter` joins downstream
//! in the real pipeline. So an `IndexedFilter` in the output can only come from
//! the eqsat seeding path, which is what this test exercises.

use mz_expr::{AccessStrategy, Id, JoinImplementation, MirRelationExpr, MirScalarExpr, func};
use mz_repr::optimize::OptimizerFeatures;
use mz_repr::{Datum, GlobalId, ReprRelationType, ReprScalarType};
use mz_transform::dataflow::DataflowMetainfo;
use mz_transform::eqsat::PhysicalEqSatTransform;
use mz_transform::{EmptyStatisticsOracle, IndexOracle, Transform, TransformCtx};

/// An [`IndexOracle`] reporting a single index on one global id.
#[derive(Debug)]
struct OneIndex {
    on: GlobalId,
    idx: GlobalId,
    key: Vec<MirScalarExpr>,
}

impl IndexOracle for OneIndex {
    fn indexes_on(
        &self,
        id: GlobalId,
    ) -> Box<dyn Iterator<Item = (GlobalId, &[MirScalarExpr])> + '_> {
        if id == self.on {
            Box::new(std::iter::once((self.idx, self.key.as_slice())))
        } else {
            Box::new(std::iter::empty())
        }
    }
}

/// Whether `expr` contains a join committed to an `IndexedFilter` implementation.
fn contains_indexed_filter(expr: &MirRelationExpr) -> bool {
    let mut found = false;
    expr.visit_pre(|n| {
        if let MirRelationExpr::Join {
            implementation: JoinImplementation::IndexedFilter(..),
            ..
        } = n
        {
            found = true;
        }
    });
    found
}

#[mz_ore::test]
fn physical_eqsat_commits_indexed_filter() {
    let on = GlobalId::Transient(1);
    let idx = GlobalId::Transient(2);
    let typ = ReprRelationType::new(vec![
        ReprScalarType::Int64.nullable(false),
        ReprScalarType::Int64.nullable(false),
    ]);
    let get = MirRelationExpr::Get {
        id: Id::Global(on),
        typ,
        access_strategy: AccessStrategy::UnknownOrLocal,
    };
    // Filter[#0 = 5](Get on), with an index on key (#0).
    let lit5 = MirScalarExpr::literal_ok(Datum::Int64(5), ReprScalarType::Int64);
    let pred = MirScalarExpr::column(0).call_binary(lit5, func::Eq);
    let mut plan = get.filter(vec![pred]);
    let input_arity = plan.arity();

    let oracle = OneIndex {
        on,
        idx,
        key: vec![MirScalarExpr::column(0)],
    };
    let features = OptimizerFeatures::default();
    let typecheck_ctx = mz_transform::typecheck::empty_typechecking_context();
    let mut df_meta = DataflowMetainfo::default();
    let mut ctx = TransformCtx::global(
        &oracle,
        &EmptyStatisticsOracle,
        &features,
        &typecheck_ctx,
        &mut df_meta,
        None,
    );

    PhysicalEqSatTransform
        .transform(&mut plan, &mut ctx)
        .expect("transform succeeds");

    assert!(
        contains_indexed_filter(&plan),
        "physical eqsat must commit an IndexedFilter join; got:\n{}",
        plan.pretty()
    );
    // The pass is arity-preserving (a semi-join keeps the input columns).
    assert_eq!(plan.arity(), input_arity, "arity must be preserved");
}

/// Without an index on the filtered `Get`, nothing is seeded and the plan keeps
/// a plain filter (no `IndexedFilter`).
#[mz_ore::test]
fn physical_eqsat_leaves_unindexed_filter_alone() {
    let on = GlobalId::Transient(1);
    let typ = ReprRelationType::new(vec![ReprScalarType::Int64.nullable(false)]);
    let get = MirRelationExpr::Get {
        id: Id::Global(on),
        typ,
        access_strategy: AccessStrategy::UnknownOrLocal,
    };
    let lit5 = MirScalarExpr::literal_ok(Datum::Int64(5), ReprScalarType::Int64);
    let pred = MirScalarExpr::column(0).call_binary(lit5, func::Eq);
    let mut plan = get.filter(vec![pred]);

    // An oracle that knows about a different relation, so no index covers `on`.
    let oracle = OneIndex {
        on: GlobalId::Transient(99),
        idx: GlobalId::Transient(2),
        key: vec![MirScalarExpr::column(0)],
    };
    let features = OptimizerFeatures::default();
    let typecheck_ctx = mz_transform::typecheck::empty_typechecking_context();
    let mut df_meta = DataflowMetainfo::default();
    let mut ctx = TransformCtx::global(
        &oracle,
        &EmptyStatisticsOracle,
        &features,
        &typecheck_ctx,
        &mut df_meta,
        None,
    );

    PhysicalEqSatTransform
        .transform(&mut plan, &mut ctx)
        .expect("transform succeeds");

    assert!(
        !contains_indexed_filter(&plan),
        "no index on the get: must not commit an IndexedFilter; got:\n{}",
        plan.pretty()
    );
}
