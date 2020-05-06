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
use differential_dataflow::trace::implementations::ord::{OrdKeySpine, OrdValSpine};
use timely::dataflow::Scope;
use timely::progress::{timestamp::Refines, Timestamp};

use dataflow_types::*;
use expr::RelationExpr;
use repr::{Row, RowArena};

use crate::operator::CollectionExt;
use crate::render::context::{ArrangementFlavor, Context};

impl<G, T> Context<G, RelationExpr, Row, T>
where
    G: Scope,
    G::Timestamp: Lattice + Refines<T>,
    T: Timestamp + Lattice,
{
    pub fn render_arrangeby(&mut self, relation_expr: &RelationExpr, id: Option<&str>) {
        if let RelationExpr::ArrangeBy { input, keys } = relation_expr {
            if keys.is_empty() {
                let collection = self.collection(input).unwrap();
                self.collections.insert(relation_expr.clone(), collection);
            }
            for key_set in keys {
                if self.arrangement(&input, &key_set).is_none() {
                    let (ok_built, err_built) = self.collection(input).unwrap();
                    let keys2 = key_set.clone();
                    let name = if let Some(id) = id {
                        format!("Arrange: {}", id)
                    } else {
                        "Arrange".to_string()
                    };
                    let (ok_collection, err_collection) = ok_built.map_fallible(move |row| {
                        let datums = row.unpack();
                        let temp_storage = RowArena::new();
                        let key_row =
                            Row::try_pack(keys2.iter().map(|k| k.eval(&datums, &temp_storage)))?;
                        Ok::<_, DataflowError>((key_row, row))
                    });
                    let err_collection = err_built.concat(&err_collection);
                    let ok_arrangement =
                        ok_collection.arrange_named::<OrdValSpine<_, _, _, _>>(&name);
                    let err_arrangement = err_collection
                        .arrange_named::<OrdKeySpine<_, _, _>>(&format!("{}-errors", name));
                    self.set_local(&input, key_set, (ok_arrangement, err_arrangement));
                }
                if self.arrangement(relation_expr, key_set).is_none() {
                    match self.arrangement(&input, key_set).unwrap() {
                        ArrangementFlavor::Local(oks, errs) => {
                            self.set_local(relation_expr, key_set, (oks, errs));
                        }
                        ArrangementFlavor::Trace(gid, oks, errs) => {
                            self.set_trace(gid, relation_expr, key_set, (oks, errs));
                        }
                    }
                }
            }
        }
    }
}
