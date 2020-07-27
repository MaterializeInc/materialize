// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use differential_dataflow::lattice::Lattice;
use differential_dataflow::Collection;

use timely::dataflow::Scope;
use timely::progress::{timestamp::Refines, Timestamp};

use dataflow_types::DataflowError;
use expr::{RelationExpr, ScalarExpr};
use repr::{Datum, Row};

use crate::operator::CollectionExt;
use crate::render::context::Context;

impl<G, T> Context<G, RelationExpr, Row, T>
where
    G: Scope,
    G::Timestamp: Lattice + Refines<T>,
    T: Timestamp + Lattice,
{
    /// Finds collection corresponding to input RelationExpr and then applies
    /// predicates to the input collection.
    pub fn render_filter(
        &mut self,
        input: &RelationExpr,
        predicates: Vec<ScalarExpr>,
    ) -> (Collection<G, Row>, Collection<G, DataflowError>) {
        let (ok_collection, err_collection) = self.collection(input).unwrap();
        let (ok_collection, new_err_collection) = render_filter_inner(ok_collection, predicates);
        let err_collection = err_collection.concat(&new_err_collection);
        (ok_collection, err_collection)
    }
}

/// Applies predicates to the given collection.
pub fn render_filter_inner<G>(
    collection: Collection<G, Row>,
    predicates: Vec<ScalarExpr>,
) -> (Collection<G, Row>, Collection<G, DataflowError>)
where
    G: Scope,
    G::Timestamp: Lattice,
{
    collection.filter_fallible(move |input_row| {
        let temp_storage = repr::RowArena::new();
        let datums = input_row.unpack();
        for p in &predicates {
            if p.eval(&datums, &temp_storage)? != Datum::True {
                return Ok(false);
            }
        }
        Ok::<_, DataflowError>(true)
    })
}
