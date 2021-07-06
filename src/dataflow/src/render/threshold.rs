// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::{Arranged, TraceAgent};
use differential_dataflow::operators::reduce::ReduceCore;
use differential_dataflow::operators::Consolidate;
use differential_dataflow::trace::implementations::ord::OrdValSpine;
use timely::dataflow::Scope;
use timely::progress::{timestamp::Refines, Timestamp};

use repr::{Diff, Row};

use crate::render::context::CollectionBundle;
use crate::render::context::{ArrangementFlavor, Context};

impl<G, T> Context<G, Row, T>
where
    G: Scope,
    G::Timestamp: Lattice + Refines<T>,
    T: Timestamp + Lattice,
{
    pub fn render_threshold(
        &mut self,
        mut input: CollectionBundle<G, Row, T>,
        arity: usize,
    ) -> CollectionBundle<G, Row, T> {
        fn threshold_arrangement<G, T, R>(
            arrangement: &R,
        ) -> Arranged<G, TraceAgent<OrdValSpine<Row, Row, G::Timestamp, Diff>>>
        where
            G: Scope,
            G::Timestamp: Lattice + Refines<T>,
            T: Timestamp + Lattice,
            R: ReduceCore<G, Row, Row, Diff>,
        {
            arrangement.reduce_abelian("Threshold", move |_k, s, t| {
                for (record, count) in s.iter() {
                    if *count < 0 {
                        t.push(((*record).clone(), *count));
                    }
                }
            })
        }

        // Arrange the input by all columns in order.
        // Different trace variants require different implementations because their
        // types are different, but the logic is identical.
        let mut all_columns = Vec::new();
        for column in 0..arity {
            all_columns.push(expr::MirScalarExpr::Column(column));
        }
        input = input.ensure_arrangements(Some(all_columns.clone()));
        let arrangement = input
            .arrangement(&all_columns)
            .expect("Arrangement ensured to exist");
        let negatives = match &arrangement {
            ArrangementFlavor::Local(oks, _) => threshold_arrangement(oks),
            ArrangementFlavor::Trace(_, oks, _) => threshold_arrangement(oks),
        };
        let (oks, errs) = arrangement.as_collection();
        let oks = negatives
            .as_collection(|k, _| k.clone())
            .negate()
            .concat(&oks)
            .consolidate();
        CollectionBundle::from_collections(oks, errs)
    }
}
