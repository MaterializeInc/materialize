// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::iter;

use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::arrangement::Arrange;
use differential_dataflow::operators::arrange::arrangement::Arranged;
use differential_dataflow::operators::join::{Join, JoinCore};
use differential_dataflow::trace::implementations::ord::OrdValSpine;
use differential_dataflow::trace::BatchReader;
use differential_dataflow::trace::Cursor;
use differential_dataflow::trace::TraceReader;
use differential_dataflow::Collection;
use timely::dataflow::Scope;
use timely::progress::{timestamp::Refines, Timestamp};

use dataflow_types::*;
use expr::{RelationExpr, ScalarExpr};
use repr::{Datum, RelationType, Row, RowArena};

use crate::operator::CollectionExt;
use crate::render::context::{ArrangementFlavor, Context};

impl<G, T> Context<G, RelationExpr, Row, T>
where
    G: Scope,
    G::Timestamp: Lattice + Refines<T>,
    T: Timestamp + Lattice,
{
    pub fn render_join(
        &mut self,
        relation_expr: &RelationExpr,
        predicates: &[ScalarExpr],
        // TODO(frank): use this argument to create a region surrounding the join.
        scope: &mut G,
        worker_index: usize,
    ) -> (Collection<G, Row>, Collection<G, DataflowError>) {
        if let RelationExpr::Join {
            inputs,
            equivalences,
            demand,
            implementation: expr::JoinImplementation::Differential((start, start_arr), order),
        } = relation_expr
        {
            let column_types = relation_expr.typ().column_types;
            let arity = column_types.len();

            // We maintain a private copy of `equivalences`, which we will digest
            // as we produce the join.
            let mut equivalences = equivalences.clone();
            for equivalence in equivalences.iter_mut() {
                equivalence.sort();
                equivalence.dedup();
            }
            let types = inputs.iter().map(|i| i.typ()).collect::<Vec<_>>();
            let arities = types
                .iter()
                .map(|t| t.column_types.len())
                .collect::<Vec<_>>();
            let mut offset = 0;
            let mut prior_arities = Vec::new();
            for input in 0..inputs.len() {
                prior_arities.push(offset);
                offset += arities[input];
            }

            // if possible, shorten input arrangements by joining them with a
            // constant row. Save the `RelationExpr` of the new shortened arrangements
            let mut filtered_inputs = inputs.clone();
            let mut filter_constants = vec![];
            for (input, keys) in iter::once((start, start_arr.as_ref()))
                .chain(order.iter().map(|(input, keys)| (input, Some(keys))))
            {
                if let Some(keys) = keys {
                    let result = self.filter_on_index_if_able(
                        &inputs[*input],
                        prior_arities[*input],
                        keys,
                        &equivalences,
                        scope,
                        worker_index,
                    );
                    if let Some((filtered_input, constants)) = result {
                        filter_constants.extend(constants);
                        filtered_inputs[*input] = filtered_input;
                    }
                }
            }
            filter_constants.sort();
            filter_constants.dedup();
            // eliminate filters that have already been applied from
            // `equivalences`
            for equivalence in equivalences.iter_mut() {
                equivalence.retain(|expr| !filter_constants.contains(expr));
            }
            equivalences.retain(|e| e.len() > 1);

            // Unwrap demand
            // TODO: If we pushed predicates into the operator, we could have a
            // more accurate view of demand that does not include the support of
            // all predicates.
            let demand = demand.clone().unwrap_or_else(|| (0..arity).collect());

            // This collection will evolve as we join in more inputs.
            let (mut joined, mut errs) = self.collection(&filtered_inputs[*start]).unwrap();

            // Maintain sources of each in-progress column.
            let mut source_columns = (prior_arities[*start]
                ..prior_arities[*start] + arities[*start])
                .collect::<Vec<_>>();

            let mut predicates = predicates.to_vec();
            if start_arr.is_none() || inputs.len() == 1 {
                // If there is no starting arrangement, then we can run filters
                // directly on the starting collection.
                // If there is only one input, we are done joining, so run filters
                let (j, es) = crate::render::delta_join::build_filter(
                    joined,
                    &source_columns,
                    &mut predicates,
                    &mut equivalences,
                );
                joined = j;
                if let Some(es) = es {
                    errs.concat(&es);
                }
            }
            for (input_index, (input, next_keys)) in order.iter().enumerate() {
                let mut next_keys_rebased = next_keys.clone();
                for expr in next_keys_rebased.iter_mut() {
                    expr.visit_mut(&mut |e| {
                        if let ScalarExpr::Column(c) = e {
                            *c += prior_arities[*input];
                        }
                    });
                }

                // Keys for the next input to be joined must be produced from
                // ScalarExprs found in `equivalences`, re-written to bind the
                // appropriate columns (as `joined` has permuted columns).
                let prev_keys = next_keys_rebased
                    .iter()
                    .map(|expr| {
                        // We expect to find `expr` in some `equivalence` which
                        // has a bound expression. Otherwise, the join plan is
                        // defective and we should panic.
                        let equivalence = equivalences
                            .iter()
                            .find(|equivs| equivs.contains(expr))
                            .expect("Expression in join plan is not in an equivalence relation");

                        // We expect to find exactly one bound expression, as
                        // multiple bound expressions should result in a filter
                        // and be removed once they have.
                        let mut bound_expr = equivalence
                            .iter()
                            .find(|expr| {
                                expr.support()
                                    .into_iter()
                                    .all(|c| source_columns.contains(&c))
                            })
                            .expect("Expression in join plan is not bound at time of use")
                            .clone();

                        bound_expr.visit_mut(&mut |e| {
                            if let ScalarExpr::Column(c) = e {
                                *c = source_columns
                                    .iter()
                                    .position(|x| x == c)
                                    .expect("Did not find bound column in source_columns");
                            }
                        });
                        bound_expr
                    })
                    .collect::<Vec<_>>();

                // We should extract each element of `next_keys` from `equivalences`,
                // as each *should* now be a redundant constraint. We do this so that
                // the demand analysis does not require these columns be produced.
                for equivalence in equivalences.iter_mut() {
                    equivalence.retain(|expr| !next_keys_rebased.contains(expr));
                }
                equivalences.retain(|e| e.len() > 1);

                // Determine which columns from `joined` and `input` should be
                // retained. Columns should be retained if they are required by
                // `demand`, or are in the support of an equivalence class.
                let mut column_demand = std::collections::HashSet::new();
                for equivalence in equivalences.iter() {
                    for expr in equivalence.iter() {
                        column_demand.extend(expr.support());
                    }
                }
                column_demand.extend(demand.iter().cloned());

                let next_vals = (0..arities[*input])
                    .filter(|c| column_demand.contains(&(prior_arities[*input] + c)))
                    .collect::<Vec<_>>();

                let next_source_vals = next_vals.clone();

                // When joining the first input, check to see if there is a
                // convenient ready-made arrangement
                let (j, es, prev_vals) = match (
                    input_index,
                    self.arrangement(&filtered_inputs[*start], &prev_keys),
                ) {
                    (0, Some(ArrangementFlavor::Local(oks, es))) => {
                        let (j, next_es) = self.differential_join(
                            oks,
                            &filtered_inputs[*input],
                            &next_keys[..],
                            next_vals,
                        );
                        (
                            j,
                            es.as_collection(|k, _v| k.clone()).concat(&next_es),
                            source_columns,
                        )
                    }
                    (0, Some(ArrangementFlavor::Trace(_gid, oks, es))) => {
                        let (j, next_es) = self.differential_join(
                            oks,
                            &filtered_inputs[*input],
                            &next_keys[..],
                            next_vals,
                        );
                        (
                            j,
                            es.as_collection(|k, _v| k.clone()).concat(&next_es),
                            source_columns,
                        )
                    }
                    _ => {
                        // Otherwise, build a new arrangement from the collection of
                        // joins of previous inputs.
                        // We exploit the demand information to restrict `prev` to
                        // its demanded columns.

                        // Identify the *indexes* of columns that are demanded by any
                        // remaining predicates and equivalence classes.
                        let prev_vals = source_columns
                            .iter()
                            .enumerate()
                            .filter_map(|(i, c)| {
                                if column_demand.contains(c) {
                                    Some(i)
                                } else {
                                    None
                                }
                            })
                            .collect::<Vec<_>>();

                        // Identify the columns we intend to retain.
                        let prev_source_vals =
                            prev_vals.iter().map(|i| source_columns[*i]).collect();

                        let (prev_keyed, es) = joined.map_fallible({
                            let mut row_packer = repr::RowPacker::new();
                            move |row| {
                                let datums = row.unpack();
                                let temp_storage = RowArena::new();
                                let key = Row::try_pack(
                                    prev_keys.iter().map(|e| e.eval(&datums, &temp_storage)),
                                )?;
                                let row = row_packer.pack(prev_vals.iter().map(|i| datums[*i]));
                                Ok((key, row))
                            }
                        });
                        let prev_keyed = prev_keyed.arrange_named::<OrdValSpine<_, _, _, _>>(
                            &format!("JoinStage: {}", input),
                        );
                        let (j, next_es) = self.differential_join(
                            prev_keyed,
                            &filtered_inputs[*input],
                            &next_keys[..],
                            next_vals,
                        );
                        (j, es.concat(&next_es), prev_source_vals)
                    }
                };

                joined = j;
                errs = errs.concat(&es);
                source_columns = prev_vals
                    .into_iter()
                    .chain(next_source_vals.iter().map(|i| prior_arities[*input] + *i))
                    .collect();

                let (j, es) = crate::render::delta_join::build_filter(
                    joined,
                    &source_columns,
                    &mut predicates,
                    &mut equivalences,
                );
                joined = j;
                if let Some(es) = es {
                    errs = errs.concat(&es);
                }
            }

            // We are obliged to produce demanded columns in order, with dummy data allowed
            // in non-demanded locations. They must all be in order, in any case. All demanded
            // columns should be present in `source_columns` (and probably not much else).

            let position_or = (0..arity)
                .map(|col| source_columns.iter().position(|c| c == &col))
                .collect::<Vec<_>>();

            (
                joined.map({
                    let mut row_packer = repr::RowPacker::new();
                    move |row| {
                        let datums = row.unpack();
                        row_packer.pack(position_or.iter().map(|pos_or| match pos_or {
                            Some(index) => datums[*index],
                            None => Datum::Dummy,
                        }))
                    }
                }),
                errs,
            )
        } else {
            panic!("render_join called on invalid expression.")
        }
    }

    /// Looks up the arrangement for the next input and joins it to the arranged
    /// version of the join of previous inputs. This is split into its own method
    /// to enable reuse of code with different types of `prev_keyed`.
    fn differential_join<J>(
        &mut self,
        prev_keyed: J,
        next_input: &RelationExpr,
        next_keys: &[ScalarExpr],
        next_vals: Vec<usize>,
    ) -> (Collection<G, Row>, Collection<G, DataflowError>)
    where
        J: JoinCore<G, Row, Row, repr::Diff>,
    {
        // Pre-test for the arrangement existence, so that we can populate it if the
        // collection is present but the arrangement is not.
        if self.arrangement(next_input, next_keys).is_none() {
            // The join may be faulty, and announce keys for an arrangement we have
            // not formed. This *shouldn't* happen, but we prefer to do something
            // sane rather than panic.
            if self.collection(next_input).is_some() {
                let arrange_by = RelationExpr::ArrangeBy {
                    input: Box::new(next_input.clone()),
                    keys: vec![next_keys.to_vec()],
                };
                self.render_arrangeby(&arrange_by, Some("MissingArrangement"));
            } else {
                panic!("Arrangement alarmingly absent!");
            }
        }

        match self.arrangement(next_input, next_keys) {
            Some(ArrangementFlavor::Local(oks, es)) => (
                self.differential_join_inner(prev_keyed, oks, next_vals),
                es.as_collection(|k, _v| k.clone()),
            ),
            Some(ArrangementFlavor::Trace(_gid, oks, es)) => (
                self.differential_join_inner(prev_keyed, oks, next_vals),
                es.as_collection(|k, _v| k.clone()),
            ),
            None => {
                unreachable!("Arrangement absent despite explicit construction");
            }
        }
    }

    /// Joins the arrangement for `next_input` to the arranged version of the
    /// join of previous inputs. This is split into its own method to enable
    /// reuse of code with different types of `next_input`.
    fn differential_join_inner<J, Tr2>(
        &mut self,
        prev_keyed: J,
        next_input: Arranged<G, Tr2>,
        next_vals: Vec<usize>,
    ) -> Collection<G, Row>
    where
        J: JoinCore<G, Row, Row, repr::Diff>,
        Tr2: TraceReader<Key = Row, Val = Row, Time = G::Timestamp, R = repr::Diff>
            + Clone
            + 'static,
        Tr2::Batch: BatchReader<Row, Tr2::Val, G::Timestamp, repr::Diff> + 'static,
        Tr2::Cursor: Cursor<Row, Tr2::Val, G::Timestamp, repr::Diff> + 'static,
    {
        let mut row_packer = repr::RowPacker::new();
        prev_keyed.join_core(&next_input, move |_keys, old, new| {
            let prev_datums = old.unpack();
            let next_datums = new.unpack();
            // TODO: We could in principle apply some predicates here, and avoid
            // constructing output rows that will be filtered out soon.
            Some(
                row_packer.pack(
                    prev_datums
                        .iter()
                        .chain(next_vals.iter().map(|i| &next_datums[*i])),
                ),
            )
        })
    }

    /// Given an input, check if it is an arrangement, and if the
    /// arrangement can be filtered for equality to a constant.
    /// If such filtering can be performed, render the filter as
    /// a differential join of an arrangement to a constant collection of
    /// size 1.
    /// Return the RelationExpr, if any, that was rendered and the constants
    /// in the rendered RelationExpr
    pub fn filter_on_index_if_able(
        &mut self,
        input: &RelationExpr,
        prior_arity: usize,
        keys: &[ScalarExpr],
        equivalences: &[Vec<ScalarExpr>],
        scope: &mut G,
        worker_index: usize,
    ) -> Option<(RelationExpr, Vec<ScalarExpr>)> {
        if let RelationExpr::ArrangeBy {
            input: inner_input, ..
        } = input
        {
            // For each key ...
            let constant_row = keys
                .iter()
                .flat_map(|k| {
                    let mut k_rebased = k.clone();
                    k_rebased.visit_mut(&mut |e| {
                        if let ScalarExpr::Column(c) = e {
                            *c += prior_arity;
                        }
                    });
                    equivalences.iter().find_map(|e| {
                        // ... look for the equivalence class containing the key ...
                        if e.contains(&k_rebased) {
                            // ... and find the constant within that equivalence
                            // class.
                            return e.iter().find_map(|s| {
                                if s.is_literal() {
                                    Some(s.clone())
                                } else {
                                    None
                                }
                            });
                        }
                        None
                    })
                })
                .collect::<Vec<_>>();

            if constant_row.len() == keys.len() {
                // We can filter the arrangement using a semijoin

                // 1. Create a RelationExpr representing the join

                // 1a. Figure out the equivalences in the join.
                let new_equivalences = keys
                    .iter()
                    .zip(constant_row.iter())
                    .map(|(key, constant)| vec![key.clone(), (*constant).clone()])
                    .collect::<Vec<_>>();

                // 1b. Create a RelationExpr representing the constant
                // Extract the datum from each ScalarExpr::Literal
                let datums = constant_row.iter().map(|s| match s.as_literal() {
                    Some(Ok(datum)) => datum,
                    Some(Err(_)) => unreachable!(""),
                    None => unreachable!(""),
                });
                let mut row_packer = repr::RowPacker::new();
                // Having the RelationType is technically not an accurate
                // representation of the expression, but relation type doesn't
                // matter when it comes to rendering.
                let constant_row_expr = RelationExpr::Constant {
                    rows: vec![(row_packer.pack(datums), 1)],
                    typ: RelationType::empty(),
                };

                // 1c. Render the constant collection
                self.render_constant(&constant_row_expr, scope, worker_index);
                let (constant_collection, errs) = self.collection(&constant_row_expr).unwrap();

                // 1d. Assemble parts of the Join `RelationExpr` together.
                let join_expr = RelationExpr::join_scalars(
                    vec![constant_row_expr, input.clone()],
                    new_equivalences,
                )
                .arrange_by(&[keys.to_vec()]);

                // 2. Render the join expression
                match self.arrangement(&inner_input, keys) {
                    Some(ArrangementFlavor::Local(oks, es)) => {
                        let result = oks.semijoin(&constant_collection).arrange_named("Semijoin");
                        let es = errs.concat(&es.as_collection(|k, _v| k.clone())).arrange();
                        self.set_local(&join_expr, keys, (result, es));
                    }
                    Some(ArrangementFlavor::Trace(_gid, oks, es)) => {
                        let result = oks.semijoin(&constant_collection).arrange_named("Semijoin");
                        let es = errs.concat(&es.as_collection(|k, _v| k.clone())).arrange();
                        self.set_local(&join_expr, keys, (result, es));
                    }
                    None => {
                        panic!("Arrangement alarmingly absent!");
                    }
                };

                // 3. Return the join expression + the vector of constants
                return Some((join_expr, constant_row));
            }
        }
        //
        None
    }
}
