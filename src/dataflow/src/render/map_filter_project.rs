// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Renders a filter expression that may reference `mz_logical_timestamp`.
//!
//! There are restricted options for how one can reference this term in
//! a maintained dataflow. Specifically, all predicates need to be of the
//! form
//! ```ignore
//! mz_logical_timestamp cmp_op expr
//! ```
//! where `cmp_op` is a comparison operator (e.g. <, >, =, >=, or <=) and
//! `expr` is an expression that does not contain `mz_logical_timestamp`.

use differential_dataflow::lattice::Lattice;
use differential_dataflow::AsCollection;
use differential_dataflow::Collection;
use timely::dataflow::Scope;
use timely::progress::{timestamp::Refines, Timestamp};

use dataflow_types::*;
use expr::{MapFilterProject, MirRelationExpr};
use repr::{Row, RowArena};

use crate::operator::StreamExt;
use crate::render::context::Context;
use crate::render::datum_vec::DatumVec;

impl<G, T> Context<G, MirRelationExpr, Row, T>
where
    G: Scope<Timestamp = repr::Timestamp>,
    G::Timestamp: Lattice + Refines<T>,
    T: Timestamp + Lattice,
{
    /// Renders a filter expression that may reference `mz_logical_timestamp`.
    ///
    /// There are restricted options for how one can reference this term in
    /// a maintained dataflow. Specifically, all predicates need to be of the
    /// form
    /// ```ignore
    /// mz_logical_timestamp cmp_op expr
    /// ```
    /// where `cmp_op` is a comparison operator (e.g. <, >, =, >=, or <=) and
    /// `expr` is an expression that does not contain `mz_logical_timestamp`.
    pub fn render_mfp_after(
        &mut self,
        mfp: MapFilterProject,
        relation_expr: &MirRelationExpr,
    ) -> (Collection<G, Row>, Collection<G, DataflowError>) {
        let plan = mfp.into_plan().unwrap_or_else(|err| {
            panic!("Temporal predicate error: {:?}", err);
        });

        let (ok_collection, err_collection) = self.collection(relation_expr).unwrap();
        let (oks, errs) = build_mfp_operator(ok_collection, plan);
        let err_collection = err_collection.concat(&errs);
        (oks, err_collection)
    }
}

pub fn build_mfp_operator<G>(
    input: Collection<G, Row>,
    plan: expr::MfpPlan,
) -> (Collection<G, Row>, Collection<G, DataflowError>)
where
    G: Scope<Timestamp = repr::Timestamp>,
{
    let (oks, err) = input.inner.flat_map_fallible({
        let mut datums = DatumVec::new();
        move |(data, time, diff)| {
            let arena = RowArena::new();
            let mut datums_local = datums.borrow_with(&data);

            let times_diffs = plan.evaluate(&mut datums_local, &arena, time, diff);
            // Drop to release borrow on `data` and allow it to move into the closure.
            drop(datums_local);
            // Each produced (time, diff) results in a copy of `data` in the output.
            // TODO: It would be nice to avoid the `data.clone` for the last output.
            times_diffs
                .map(move |time_diff| time_diff.map_err(|(e, t, d)| (DataflowError::from(e), t, d)))
        }
    });

    (oks.as_collection(), err.as_collection())
}
