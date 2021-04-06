// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::arrangement::Arrange;
use differential_dataflow::operators::arrange::arrangement::ArrangeByKey;
use differential_dataflow::trace::implementations::ord::OrdValSpine;
use timely::dataflow::Scope;
use timely::progress::{timestamp::Refines, Timestamp};

use expr::MirRelationExpr;
use repr::Row;

use crate::render::context::{ArrangementFlavor, Context};

impl<G, T> Context<G, MirRelationExpr, Row, T>
where
    G: Scope,
    G::Timestamp: Lattice + Refines<T>,
    T: Timestamp + Lattice,
{
    pub fn render_threshold(&mut self, relation_expr: &MirRelationExpr) {
        if let MirRelationExpr::Threshold { input } = relation_expr {
            // TODO: re-use and publish arrangement here.
            let arity = input.arity();
            let keys = (0..arity).collect::<Vec<_>>();

            // TODO: easier idioms for detecting, re-using, and stashing.
            if self.arrangement_columns(&input, &keys[..]).is_none() {
                // self.ensure_rendered(input, scope, worker_index);
                let (ok_built, err_built) = self.collection(input).unwrap();
                let keys2 = keys.clone();
                let ok_keyed = ok_built
                    .map({
                        move |row| {
                            let datums = row.unpack();
                            let iterator = keys2.iter().map(|i| datums[*i]);
                            let total_size = repr::datums_size(iterator.clone());
                            let mut key_row = Row::with_capacity(total_size);
                            key_row.extend(iterator);
                            (key_row, row)
                        }
                    })
                    .arrange_by_key();
                self.set_local_columns(&input, &keys[..], (ok_keyed, err_built.arrange()));
            }

            use differential_dataflow::operators::reduce::ReduceCore;

            let (ok_arranged, err_arranged) = match self.arrangement_columns(&input, &keys[..]) {
                Some(ArrangementFlavor::Local(oks, errs)) => (
                    oks.reduce_abelian::<_, OrdValSpine<_, _, _, _>>(
                        "Threshold",
                        move |_k, s, t| {
                            for (record, count) in s.iter() {
                                if *count > 0 {
                                    t.push(((*record).clone(), *count));
                                }
                            }
                        },
                    ),
                    errs,
                ),
                Some(ArrangementFlavor::Trace(_gid, oks, errs)) => (
                    oks.reduce_abelian::<_, OrdValSpine<_, _, _, _>>(
                        "Threshold",
                        move |_k, s, t| {
                            for (record, count) in s.iter() {
                                if *count > 0 {
                                    t.push(((*record).clone(), *count));
                                }
                            }
                        },
                    ),
                    errs.as_collection(|k, _v| k.clone()).arrange(),
                ),
                None => {
                    panic!("Arrangement alarmingly absent!");
                }
            };

            let index = (0..keys.len()).collect::<Vec<_>>();
            self.set_local_columns(relation_expr, &index[..], (ok_arranged, err_arranged));
        }
    }
}
