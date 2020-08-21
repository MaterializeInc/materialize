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
use differential_dataflow::operators::arrange::arrangement::Arranged;
use differential_dataflow::operators::join::JoinCore;
use differential_dataflow::trace::implementations::ord::OrdValSpine;
use differential_dataflow::trace::BatchReader;
use differential_dataflow::trace::Cursor;
use differential_dataflow::trace::TraceReader;
use differential_dataflow::Collection;
use timely::dataflow::Scope;
use timely::progress::{timestamp::Refines, Timestamp};

use dataflow_types::*;
use expr::{RelationExpr, ScalarExpr};
use repr::{Datum, Row, RowArena};

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
        _scope: &mut G,
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

            // Unwrap demand
            // TODO: If we pushed predicates into the operator, we could have a
            // more accurate view of demand that does not include the support of
            // all predicates.
            let demand = demand.clone().unwrap_or_else(|| (0..arity).collect());

            // This collection will evolve as we join in more inputs.
            let (mut joined, mut errs) = self.collection(&inputs[*start]).unwrap();

            // Maintain sources of each in-progress column.
            let mut source_columns = (prior_arities[*start]
                ..prior_arities[*start] + arities[*start])
                .collect::<Vec<_>>();

            let mut predicates = predicates.to_vec();
            if start_arr.is_none() {
                // If there is no starting arrangement, then we can run filters
                // directly on the starting collection.
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

                let (j, es) = if input_index == 0 && start_arr.is_some() {
                    // Identify the columns we intend to retain.
                    source_columns = source_columns
                        .into_iter()
                        .chain(next_vals.iter().map(|i| prior_arities[*input] + *i))
                        .collect();
                    // if a starting arrangement has been specified, use that
                    let start_arr = start_arr.as_ref().unwrap();
                    match self.arrangement(&inputs[*start], start_arr) {
                        Some(ArrangementFlavor::Local(oks, es)) => {
                            let (j, next_es) = self.differential_join(
                                oks,
                                &inputs[*input],
                                &next_keys[..],
                                next_vals,
                            );
                            (j, es.as_collection(|k, _v| k.clone()).concat(&next_es))
                        }
                        Some(ArrangementFlavor::Trace(_gid, oks, es)) => {
                            let (j, next_es) = self.differential_join(
                                oks,
                                &inputs[*input],
                                &next_keys[..],
                                next_vals,
                            );
                            (j, es.as_collection(|k, _v| k.clone()).concat(&next_es))
                        }
                        None => {
                            unreachable!("Start arrangement not present");
                        }
                    }
                } else {
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
                    source_columns = prev_vals
                        .iter()
                        .map(|i| source_columns[*i])
                        .chain(next_vals.iter().map(|i| prior_arities[*input] + *i))
                        .collect();

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
                    let prev_keyed = prev_keyed
                        .arrange_named::<OrdValSpine<_, _, _, _>>(&format!("JoinStage: {}", input));
                    let (j, next_es) = self.differential_join(
                        prev_keyed,
                        &inputs[*input],
                        &next_keys[..],
                        next_vals,
                    );
                    (j, es.concat(&next_es))
                };

                joined = j;
                errs = errs.concat(&es);

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
        J: JoinCore<G, Row, Row, expr::Diff>,
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
        J: JoinCore<G, Row, Row, expr::Diff>,
        Tr2: TraceReader<Key = Row, Val = Row, Time = G::Timestamp, R = expr::Diff>
            + Clone
            + 'static,
        Tr2::Batch: BatchReader<Row, Tr2::Val, G::Timestamp, expr::Diff> + 'static,
        Tr2::Cursor: Cursor<Row, Tr2::Val, G::Timestamp, expr::Diff> + 'static,
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
}
