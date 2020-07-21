// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use differential_dataflow::lattice::Lattice;
use differential_dataflow::{AsCollection, Collection};

use timely::dataflow::operators::map::Map;
use timely::dataflow::operators::to_stream::ToStream;
use timely::dataflow::Scope;
use timely::progress::{timestamp::Refines, Timestamp};

use expr::RelationExpr;
use repr::Row;

use crate::operator::CollectionExt;
use crate::render::context::Context;

impl<G, T> Context<G, RelationExpr, Row, T>
where
    G: Scope,
    G::Timestamp: Lattice + Refines<T>,
    T: Timestamp + Lattice,
{
    pub fn render_constant(
        &mut self,
        relation_expr: &RelationExpr,
        scope: &mut G,
        worker_index: usize,
    ) {
        // The constant collection is instantiated only on worker zero.
        if let RelationExpr::Constant { rows, .. } = relation_expr {
            let rows = if worker_index == 0 {
                rows.clone()
            } else {
                vec![]
            };

            let collection = rows
                .to_stream(scope)
                .map(|(x, diff)| (x, timely::progress::Timestamp::minimum(), diff))
                .as_collection();

            let err_collection = Collection::empty(scope);

            self.collections
                .insert(relation_expr.clone(), (collection, err_collection));
        }
    }
}
