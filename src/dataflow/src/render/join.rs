// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

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
use expr::{MapFilterProject, RelationExpr, ScalarExpr};
use repr::{Datum, Row, RowArena, RowPacker};

use crate::operator::CollectionExt;
use crate::render::context::{ArrangementFlavor, Context};
use crate::render::datum_vec::DatumVec;
use crate::render::delta_join::MyClosure;

impl<G, T> Context<G, RelationExpr, Row, T>
where
    G: Scope,
    G::Timestamp: Lattice + Refines<T>,
    T: Timestamp + Lattice,
{
    pub fn render_join(
        &mut self,
        relation_expr: &RelationExpr,
        map_filter_project: MapFilterProject,
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
            let input_mapper = expr::JoinInputMapper::new(inputs);
            let output_arity = input_mapper.total_columns();

            // Determine dummy columns for un-demanded outputs, and a projection.
            let (dummies, demand_projection) = if let Some(demand) = demand {
                let mut dummies = Vec::new();
                let mut demand_projection = Vec::new();
                for (column, typ) in relation_expr.typ().column_types.into_iter().enumerate() {
                    if demand.contains(&column) {
                        demand_projection.push(column);
                    } else {
                        demand_projection.push(output_arity + dummies.len());
                        dummies.push(ScalarExpr::literal_ok(Datum::Dummy, typ));
                    }
                }
                (dummies, demand_projection)
            } else {
                (Vec::new(), (0..output_arity).collect::<Vec<_>>())
            };

            let (map, filter, project) = map_filter_project.as_map_filter_project();

            let map_filter_project = MapFilterProject::new(output_arity)
                .map(dummies)
                .project(demand_projection)
                .map(map)
                .filter(filter)
                .project(project);

            // Other than the stream of updates, our loop-carried state are these
            // three variables: `column_map`, `equivalences`, and `mfp`, which
            // record the current locations of extended output columns, and what
            // work remains to be done on them (other than the joining itself).
            let mut column_map = HashMap::new();
            for column in input_mapper.global_columns(*start) {
                column_map.insert(column, column_map.len());
            }
            // We maintain a private copy of `equivalences`, which we will digest
            // as we produce the join.
            let mut equivalences = equivalences.clone();
            for equivalence in equivalences.iter_mut() {
                equivalence.sort();
                equivalence.dedup();
            }
            // We maintain a private copy of `map_filter_project`, which we will
            // digest as we produce the join.
            let mut mfp = map_filter_project;

            // This collection will evolve as we join in more inputs.
            // TODO(mcsherry): determine and apply closure here in `flat_map_ref` form.
            // TODO(mcsherry): If we plan to use an arrangement, should one exist, then
            // this is wasteful as it instantiates all rows which are then dropped.
            let (mut joined, mut errs) = self.collection(&inputs[*start]).unwrap();

            let use_leading_arrangement = start_arr.is_some() && inputs.len() > 1;
            if !use_leading_arrangement {
                // NOTE(mcsherry): ideally this code is rarely/never relevant, as the associated logic
                // could be pushed down to the input and perhaps beyond. I'm not certain under what
                // circumstance we should just delete it, though.

                // At this point we are able to construct a per-row closure that can be applied once
                // we have the first wave of columns in place. We will not apply it quite yet, because
                // we have three code paths that might produce data and it is complicated.
                let closure = MyClosure::build(&mut column_map, &mut equivalences, &mut mfp);

                // If there is no starting arrangement, then we can run filters
                // directly on the starting collection.
                // If there is only one input, we are done joining, so run filters
                let (j, es) = joined.flat_map_fallible({
                    // Reuseable allocation for unpacking.
                    let mut datums = DatumVec::new();
                    let mut row_packer = RowPacker::new();
                    move |row| {
                        let temp_storage = RowArena::new();
                        let mut datums_local = datums.borrow_with(&row);
                        // TODO(mcsherry): re-use `row` allocation.
                        closure
                            .apply(&mut datums_local, &temp_storage, &mut row_packer)
                            .map_err(DataflowError::from)
                            .transpose()
                    }
                });
                joined = j;
                errs.concat(&es);
            }

            // We track the input relations as they are
            // added to the join so we can figure out
            // which expressions have been bound.
            let mut bound_inputs = vec![*start];
            for (input_index, (input, next_keys)) in order.iter().enumerate() {
                let next_keys_rebased = next_keys
                    .iter()
                    .map(|k| input_mapper.map_expr_to_global(k.clone(), *input))
                    .collect::<Vec<_>>();

                // Keys for the next input to be joined must be produced from
                // ScalarExprs found in `equivalences`, re-written to bind the
                // appropriate columns (as `joined` has permuted columns).
                let prev_keys = next_keys_rebased
                    .iter()
                    .map(|expr| {
                        let mut bound_expr = input_mapper
                            .find_bound_expr(expr, &bound_inputs, &equivalences)
                            .expect("Expression in join plan is not bound at time of use");

                        bound_expr.permute_map(&column_map);
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

                // Update our map of the sources of each column in the update stream.
                for column in input_mapper.global_columns(*input) {
                    column_map.insert(column, column_map.len());
                }

                // At this point we are able to construct a per-row closure that can be applied
                // once we have added additional columns from `lookup`. We build it now so that
                // it can be applied immediately in the `lookup` operator.
                let closure = MyClosure::build(&mut column_map, &mut equivalences, &mut mfp);

                // When joining the first input, check to see if we are meant to use an existing
                // arrangement.
                let (j, es) = match (
                    input_index,
                    use_leading_arrangement,
                    self.arrangement(&inputs[*start], &prev_keys),
                ) {
                    (0, true, Some(ArrangementFlavor::Local(oks, es))) => {
                        let (j, next_es) =
                            self.differential_join(oks, &inputs[*input], &next_keys[..], closure);
                        (j, es.as_collection(|k, _v| k.clone()).concat(&next_es))
                    }
                    (0, true, Some(ArrangementFlavor::Trace(_gid, oks, es))) => {
                        let (j, next_es) =
                            self.differential_join(oks, &inputs[*input], &next_keys[..], closure);
                        (j, es.as_collection(|k, _v| k.clone()).concat(&next_es))
                    }
                    _ => {
                        // Otherwise, build a new arrangement from the collection of
                        // joins of previous inputs.
                        // We exploit the demand information to restrict `prev` to
                        // its demanded columns.
                        let (prev_keyed, es) = joined.map_fallible({
                            // Reuseable allocation for unpacking.
                            let mut datums = DatumVec::new();
                            move |row| {
                                let temp_storage = RowArena::new();
                                let datums_local = datums.borrow_with(&row);
                                let key = Row::try_pack(
                                    prev_keys
                                        .iter()
                                        .map(|e| e.eval(&datums_local, &temp_storage)),
                                )?;
                                // Explicit drop here to allow `row` to be returned.
                                drop(datums_local);
                                // TODO(mcsherry): We could remove any columns used only for `key`.
                                // This cannot be done any earlier, for example in a prior closure,
                                // because we need the columns for key production.
                                Ok((key, row))
                            }
                        });
                        let prev_keyed = prev_keyed.arrange_named::<OrdValSpine<_, _, _, _>>(
                            &format!("JoinStage-input{}", input),
                        );
                        let (j, next_es) = self.differential_join(
                            prev_keyed,
                            &inputs[*input],
                            &next_keys[..],
                            closure,
                        );
                        (j, es.concat(&next_es))
                    }
                };

                joined = j;
                errs = errs.concat(&es);
                bound_inputs.push(*input);
            }

            // We must now apply `mfp` as it *should* have sufficient support for
            // full evaluation. Before we do this, we need to permute it to refer
            // to the correct physical column locations.
            mfp.permute(&column_map, column_map.len());
            // The `mfp` has access to all columns at this point, and could
            // plausibly be much simpler that a general `MapFilterProject`.
            // As of this writing, the `mfp` will only contain literals and
            // some projection to place them and potentially copy columns.
            // These should not error (if the literals are non-errors) and
            // could result in a simpler (non-erroring) operator.
            if !mfp.is_identity() {
                let (updates, errors) = joined.flat_map_fallible({
                    // Reuseable allocation for unpacking.
                    let mut datums = DatumVec::new();
                    let mut row_packer = repr::RowPacker::new();
                    move |row| {
                        let temp_storage = RowArena::new();
                        let mut datums_local = datums.borrow_with(&row);
                        // TODO(mcsherry): re-use `row` allocation.
                        mfp.evaluate(&mut datums_local, &temp_storage, &mut row_packer)
                            .map_err(DataflowError::from)
                            .transpose()
                    }
                });

                joined = updates;
                errs = errs.concat(&errors);
            }

            (joined, errs)
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
        closure: MyClosure,
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
            Some(ArrangementFlavor::Local(oks, es)) => {
                let (oks, err) = self.differential_join_inner(prev_keyed, oks, closure);
                (oks, err.concat(&es.as_collection(|k, _v| k.clone())))
            }
            Some(ArrangementFlavor::Trace(_gid, oks, es)) => {
                let (oks, err) = self.differential_join_inner(prev_keyed, oks, closure);
                (oks, err.concat(&es.as_collection(|k, _v| k.clone())))
            }
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
        closure: MyClosure,
    ) -> (Collection<G, Row>, Collection<G, DataflowError>)
    where
        J: JoinCore<G, Row, Row, repr::Diff>,
        Tr2: TraceReader<Key = Row, Val = Row, Time = G::Timestamp, R = repr::Diff>
            + Clone
            + 'static,
        Tr2::Batch: BatchReader<Row, Tr2::Val, G::Timestamp, repr::Diff> + 'static,
        Tr2::Cursor: Cursor<Row, Tr2::Val, G::Timestamp, repr::Diff> + 'static,
    {
        use differential_dataflow::AsCollection;
        use timely::dataflow::operators::OkErr;

        // Reuseable allocation for unpacking.
        let mut datums = DatumVec::new();
        let mut row_packer = RowPacker::new();
        let (oks, err) = prev_keyed
            .join_core(&next_input, move |_keys, old, new| {
                let temp_storage = RowArena::new();
                let mut datums_local = datums.borrow();
                datums_local.extend(old.iter());
                datums_local.extend(new.iter());

                closure
                    .apply(&mut datums_local, &temp_storage, &mut row_packer)
                    .map_err(DataflowError::from)
                    .transpose()
            })
            .inner
            .ok_err(|(x, t, d)| {
                // TODO(mcsherry): consider `ok_err()` for `Collection`.
                match x {
                    Ok(x) => Ok((x, t, d)),
                    Err(x) => Err((x, t, d)),
                }
            });

        (oks.as_collection(), err.as_collection())
    }
}
