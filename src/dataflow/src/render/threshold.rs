// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::reduce::ReduceCore;
use differential_dataflow::trace::implementations::ord::OrdValSpine;
use timely::dataflow::Scope;
use timely::progress::{timestamp::Refines, Timestamp};

use repr::Row;

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
        // Arrange the input by all columns in order.
        // Different trace variants require different implementations because their
        // types are different, but the logic is identical.
        let mut all_columns = Vec::new();
        for column in 0..arity {
            all_columns.push(expr::MirScalarExpr::Column(column));
        }
        input = input.ensure_arrangements(Some(all_columns.clone()));
        match input
            .arrangement(&all_columns)
            .expect("Arrangement ensured to exist")
        {
            ArrangementFlavor::Local(oks, errs) => {
                let oks = oks.reduce_abelian::<_, OrdValSpine<_, _, _, _>>(
                    "Threshold",
                    move |_k, s, t| {
                        for (record, count) in s.iter() {
                            if *count > 0 {
                                t.push(((*record).clone(), *count));
                            }
                        }
                    },
                );
                CollectionBundle::from_columns(0..arity, ArrangementFlavor::Local(oks, errs))
            }
            ArrangementFlavor::Trace(_, oks, errs) => {
                let oks = oks.reduce_abelian::<_, OrdValSpine<_, _, _, _>>(
                    "Threshold",
                    move |_k, s, t| {
                        for (record, count) in s.iter() {
                            if *count > 0 {
                                t.push(((*record).clone(), *count));
                            }
                        }
                    },
                );
                use differential_dataflow::operators::arrange::ArrangeBySelf;
                let errs = errs.as_collection(|k, _| k.clone()).arrange_by_self();
                CollectionBundle::from_columns(0..arity, ArrangementFlavor::Local(oks, errs))
            }
        }
    }
}
