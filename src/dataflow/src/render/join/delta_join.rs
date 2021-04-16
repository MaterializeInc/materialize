// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Delta join execution planning and dataflow construction.
//!
//! Delta joins are a join over multiple input relations, implemented by an
//! independent dataflow path for each input. Each path is joined against the
//! other inputs using a "lookup" operator, and the path results are collected
//! and return as the output for the entire dataflow.
//!
//! This implementation strategy allows us to re-use existing arrangements, and
//! not create any new stateful operators.

#![allow(clippy::op_ref)]
use differential_dataflow::lattice::Lattice;
use dogsdogsdogs::altneu::AltNeu;
use std::collections::HashSet;
use timely::dataflow::Scope;

use dataflow_types::DataflowError;
use expr::{JoinInputMapper, MapFilterProject, MirRelationExpr, MirScalarExpr};
use repr::{Datum, Row, RowArena, Timestamp};

use super::super::context::{ArrangementFlavor, Context};
use crate::operator::CollectionExt;
use crate::render::datum_vec::DatumVec;
use crate::render::join::{JoinBuildState, JoinClosure};

/// A delta query is implemented by a set of paths, one for each input.
///
/// Each delta query path responds to its input changes by repeated lookups
/// in arrangements for other join inputs. These lookups require specific
/// instructions about which expressions to use as keys. Along the way,
/// various closures are applied to filter and project as early as possible.
pub struct DeltaJoinPlan {
    /// The set of path plans.
    ///
    /// Each path identifies its source relation, so the order is only
    /// important for determinism of dataflow construction.
    path_plans: Vec<DeltaPathPlan>,
}

/// A delta query path is implemented by a sequences of stages,
pub struct DeltaPathPlan {
    /// The relation whose updates seed the dataflow path.
    source_relation: usize,
    /// An initial closure to apply before any stages.
    ///
    /// Values of `None` indicate the identity closure.
    initial_closure: Option<JoinClosure>,
    /// A *sequence* of stages to apply one after the other.
    stage_plans: Vec<DeltaStagePlan>,
    /// A concluding closure to apply after the last stage.
    ///
    /// Values of `None` indicate the identity closure.
    final_closure: Option<JoinClosure>,
}

/// A delta query stage performs a stream lookup into an arrangement.
pub struct DeltaStagePlan {
    /// The relation index into which we will look up.
    lookup_relation: usize,
    /// The key expressions to use for the streamed relation.
    ///
    /// While this starts as a stream of the source relation,
    /// it evolves through multiple lookups and ceases to be
    /// the same thing, hence the different name.
    stream_key: Vec<MirScalarExpr>,
    /// The key expressions to use for the lookup relation.
    lookup_key: Vec<MirScalarExpr>,
    /// The closure to apply to the concatenation of columns
    /// of the stream and lookup relations.
    closure: JoinClosure,
}

impl DeltaJoinPlan {
    /// Create a new join plan from the required arguments.
    pub fn create_from(
        equivalences: &[Vec<MirScalarExpr>],
        join_orders: &[Vec<(usize, Vec<MirScalarExpr>)>],
        input_mapper: JoinInputMapper,
        map_filter_project: MapFilterProject,
    ) -> Self {
        let number_of_inputs = join_orders.len();

        // Create an empty plan, with capacity for the intended number of path plans.
        let mut join_plan = DeltaJoinPlan {
            path_plans: Vec::with_capacity(number_of_inputs),
        };

        // Each source relation will contribute a path to the join plan.
        for source_relation in 0..number_of_inputs {
            // Construct initial join build state.
            // This state will evolves as we build the join dataflow.
            let mut join_build_state = JoinBuildState::new(
                input_mapper.global_columns(source_relation),
                &equivalences,
                &map_filter_project,
            );

            // Initial action we can take on the source relation before joining.
            let initial_closure = join_build_state.extract_closure();
            let initial_closure = if initial_closure.is_identity() {
                None
            } else {
                Some(initial_closure)
            };

            // Sequence of steps to apply.
            let mut stage_plans = Vec::with_capacity(number_of_inputs - 1);

            // We track the input relations as they are added to the join so we can figure out
            // which expressions have been bound.
            let mut bound_inputs = vec![source_relation];
            // We use the order specified by the implementation.
            let order = &join_orders[source_relation];

            for (lookup_relation, lookup_key) in order.iter() {
                // rebase the intended key to use global column identifiers.
                let lookup_key_rebased = lookup_key
                    .iter()
                    .map(|k| input_mapper.map_expr_to_global(k.clone(), *lookup_relation))
                    .collect::<Vec<_>>();

                // Expressions to use as a key for the stream of incoming updates
                // are determined by locating the elements of `lookup_key` among
                // the existing bound `columns`. If that cannot be done, the plan
                // is irrecoverably defective and we panic.
                // TODO: explicitly validate this before rendering.
                let stream_key = lookup_key_rebased
                    .iter()
                    .map(|expr| {
                        let mut bound_expr = input_mapper
                            .find_bound_expr(expr, &bound_inputs, &join_build_state.equivalences)
                            .expect("Expression in join plan is not bound at time of use");
                        // Rewrite column references to physical locations.
                        bound_expr.permute_map(&join_build_state.column_map);
                        bound_expr
                    })
                    .collect::<Vec<_>>();

                // Introduce new columns and expressions they enable. Form a new closure.
                let closure = join_build_state.add_columns(
                    input_mapper.global_columns(*lookup_relation),
                    &lookup_key_rebased,
                );

                bound_inputs.push(*lookup_relation);
                // record the stage plan as next in the path.
                stage_plans.push(DeltaStagePlan {
                    lookup_relation: *lookup_relation,
                    stream_key,
                    lookup_key: lookup_key.clone(),
                    closure,
                });
            }
            // determine a final closure, and complete the path plan.
            let final_closure = join_build_state.complete();
            let final_closure = if final_closure.is_identity() {
                None
            } else {
                Some(final_closure)
            };

            // Insert the path plan.
            join_plan.path_plans.push(DeltaPathPlan {
                source_relation,
                initial_closure,
                stage_plans,
                final_closure,
            });
        }

        join_plan
    }
}

impl<G> Context<G, MirRelationExpr, Row, Timestamp>
where
    G: Scope<Timestamp = Timestamp>,
{
    /// Renders `MirRelationExpr:Join` using dogs^3 delta query dataflows.
    ///
    /// The join is followed by the application of `map_filter_project`, whose
    /// implementation will be pushed in to the join pipeline if at all possible.
    pub fn render_delta_join<F>(
        &mut self,
        relation_expr: &MirRelationExpr,
        map_filter_project: MapFilterProject,
        scope: &mut G,
        worker_index: usize,
        subtract: F,
    ) -> (Collection<G, Row>, Collection<G, DataflowError>)
    where
        F: Fn(&G::Timestamp) -> G::Timestamp + Clone + 'static,
    {
        if let MirRelationExpr::Join {
            inputs,
            equivalences,
            demand: _,
            implementation: expr::JoinImplementation::DeltaQuery(orders),
        } = relation_expr
        {
            // Step one is to plan the execution of the delta query.
            let input_mapper = JoinInputMapper::new(inputs);
            let join_plan = DeltaJoinPlan::create_from(
                equivalences,
                &orders[..],
                input_mapper,
                map_filter_project,
            );

            for input in inputs.iter() {
                self.ensure_rendered(input, scope, worker_index);
            }

            // Collects error streams for the ambient scope.
            let mut scope_errs = Vec::new();

            // Deduplicate the error streams of multiply used arrangements.
            let mut local_err_dedup = HashSet::new();
            let mut trace_err_dedup = HashSet::new();

            // We'll need a new scope, to hold `AltNeu` wrappers, and we'll want
            // to import all traces as alt and neu variants (unless we do a more
            // careful analysis).
            let results =
                scope
                    .clone()
                    .scoped::<AltNeu<G::Timestamp>, _, _>("delta query", |inner| {
                        // Our plan is to iterate through each input relation, and attempt
                        // to find a plan that maximally uses existing keys (better: uses
                        // existing arrangements, to which we have access).
                        let mut join_results = Vec::new();

                        // First let's prepare the input arrangements we will need.
                        // This reduces redundant imports, and simplifies the dataflow structure.
                        // As the arrangements are all shared, it should not dramatically improve
                        // the efficiency, but the dataflow simplification is worth doing.
                        //
                        // The arrangements are keyed by input and arrangement key, and by whether
                        // the arrangement is "alt" or "neu", which corresponds to whether the use
                        // of the arrangement is by a relation before or after it in the order, resp.
                        // Because the alt and neu variants have different types, we will maintain
                        // them in different collections.
                        let mut arrangements_alt = std::collections::HashMap::new();
                        let mut arrangements_neu = std::collections::HashMap::new();
                        for relation in 0..inputs.len() {
                            let order = &orders[relation];
                            for (other, lookup_key) in order.iter() {
                                let subtract = subtract.clone();
                                // Alt case
                                if other > &relation {
                                    arrangements_alt
                                        .entry((&inputs[*other], &lookup_key[..]))
                                        .or_insert_with(|| {
                                            match self
                                                .arrangement(&inputs[*other], &lookup_key[..])
                                                .unwrap_or_else(|| {
                                                    panic!(
                                                        "Arrangement alarmingly absent!: {}, {:?}",
                                                        inputs[*other].pretty(),
                                                        &lookup_key[..]
                                                    )
                                                }) {
                                                ArrangementFlavor::Local(oks, errs) => {
                                                    if local_err_dedup
                                                        .insert((&inputs[*other], &lookup_key[..]))
                                                    {
                                                        scope_errs.push(
                                                            errs.as_collection(|k, _v| k.clone()),
                                                        );
                                                    }
                                                    Ok(oks.enter_at(
                                                        inner,
                                                        |_, _, t| AltNeu::alt(t.clone()),
                                                        move |t| subtract(&t.time),
                                                    ))
                                                }
                                                ArrangementFlavor::Trace(_gid, oks, errs) => {
                                                    if trace_err_dedup
                                                        .insert((&inputs[*other], &lookup_key[..]))
                                                    {
                                                        scope_errs.push(
                                                            errs.as_collection(|k, _v| k.clone()),
                                                        );
                                                    }
                                                    Err(oks.enter_at(
                                                        inner,
                                                        |_, _, t| AltNeu::alt(t.clone()),
                                                        move |t| subtract(&t.time),
                                                    ))
                                                }
                                            }
                                        });
                                } else {
                                    arrangements_neu
                                        .entry((&inputs[*other], &lookup_key[..]))
                                        .or_insert_with(|| {
                                            match self
                                                .arrangement(&inputs[*other], &lookup_key[..])
                                                .unwrap_or_else(|| {
                                                    panic!(
                                                        "Arrangement alarmingly absent!: {}, {:?}",
                                                        inputs[*other].pretty(),
                                                        &lookup_key[..]
                                                    )
                                                }) {
                                                ArrangementFlavor::Local(oks, errs) => {
                                                    if local_err_dedup
                                                        .insert((&inputs[*other], &lookup_key[..]))
                                                    {
                                                        scope_errs.push(
                                                            errs.as_collection(|k, _v| k.clone()),
                                                        );
                                                    }
                                                    Ok(oks.enter_at(
                                                        inner,
                                                        |_, _, t| AltNeu::neu(t.clone()),
                                                        move |t| subtract(&t.time),
                                                    ))
                                                }
                                                ArrangementFlavor::Trace(_gid, oks, errs) => {
                                                    if trace_err_dedup
                                                        .insert((&inputs[*other], &lookup_key[..]))
                                                    {
                                                        scope_errs.push(
                                                            errs.as_collection(|k, _v| k.clone()),
                                                        );
                                                    }
                                                    Err(oks.enter_at(
                                                        inner,
                                                        |_, _, t| AltNeu::neu(t.clone()),
                                                        move |t| subtract(&t.time),
                                                    ))
                                                }
                                            }
                                        });
                                }
                            }
                        }

                        // Collects error streams for the inner scope. Concats before leaving.
                        let mut inner_errs = Vec::with_capacity(inputs.len());
                        for path_plan in join_plan.path_plans.into_iter() {
                            // Deconstruct the stages of the path plan.
                            let DeltaPathPlan {
                                source_relation,
                                initial_closure,
                                stage_plans,
                                final_closure,
                            } = path_plan;

                            // This collection determines changes that result from updates inbound
                            // from `inputs[relation]` and reflects all strictly prior updates and
                            // concurrent updates from relations prior to `relation`.
                            let name = format!("delta path {}", source_relation);
                            let path_results = inner.clone().region_named(&name, |region| {
                                // The plan is to move through each relation, starting from `relation` and in the order
                                // indicated in `orders[relation]`. At each moment, we will have the columns from the
                                // subset of relations encountered so far, and we will have applied as much as we can
                                // of the filters in `equivalences` and the logic in `map_filter_project`, based on the
                                // available columns.
                                //
                                // As we go, we will track the physical locations of each intended output column, as well
                                // as the locations of intermediate results from partial application of `map_filter_project`.
                                //
                                // Just before we apply the `lookup` function to perform a join, we will first use our
                                // available information to determine the filtering and logic that we can apply, and
                                // introduce that in to the `lookup` logic to cause it to happen in that operator.

                                // Collects error streams for the region scope. Concats before leaving.
                                let mut region_errs = Vec::with_capacity(inputs.len());

                                // Ensure this input is rendered, and extract its update stream.
                                let mut update_stream = if let Some((_key, val)) = arrangements_alt
                                    .iter()
                                    .find(|(key, _val)| key.0 == &inputs[source_relation])
                                {
                                    match val {
                                        Ok(local) => local
                                            .as_collection(|_k, v| v.clone())
                                            .enter_region(region),
                                        Err(trace) => trace
                                            .as_collection(|_k, v| v.clone())
                                            .enter_region(region),
                                    }
                                } else {
                                    self.collection(&inputs[source_relation])
                                        .expect("Failed to render update stream")
                                        .0
                                        .enter(inner)
                                        .enter_region(region)
                                };

                                // Apply what `closure` we are able to, and record any errors.
                                if let Some(initial_closure) = initial_closure {
                                    let (stream, errs) = update_stream.flat_map_fallible({
                                        let mut datums = DatumVec::new();
                                        move |row| {
                                            let temp_storage = RowArena::new();
                                            let mut datums_local = datums.borrow_with(&row);
                                            // TODO(mcsherry): re-use `row` allocation.
                                            initial_closure
                                                .apply(&mut datums_local, &temp_storage)
                                                .transpose()
                                        }
                                    });
                                    update_stream = stream;
                                    region_errs.push(errs.map(DataflowError::from));
                                }

                                // Repeatedly update `update_stream` to reflect joins with more and more
                                // other relations, in the specified order.
                                for stage_plan in stage_plans.into_iter() {
                                    let DeltaStagePlan {
                                        lookup_relation,
                                        stream_key,
                                        lookup_key,
                                        closure,
                                    } = stage_plan;

                                    // We require different logic based on the flavor of arrangement.
                                    // We may need to cache each of these if we want to re-use the same wrapped
                                    // arrangement, rather than re-wrap each time we use a thing.
                                    let (oks, errs) = if lookup_relation > source_relation {
                                        match arrangements_alt
                                            .get(&(&inputs[lookup_relation], &lookup_key[..]))
                                            .unwrap()
                                        {
                                            Ok(local) => build_lookup(
                                                update_stream,
                                                local.enter_region(region),
                                                stream_key,
                                                closure,
                                            ),
                                            Err(trace) => build_lookup(
                                                update_stream,
                                                trace.enter_region(region),
                                                stream_key,
                                                closure,
                                            ),
                                        }
                                    } else {
                                        match arrangements_neu
                                            .get(&(&inputs[lookup_relation], &lookup_key[..]))
                                            .unwrap()
                                        {
                                            Ok(local) => build_lookup(
                                                update_stream,
                                                local.enter_region(region),
                                                stream_key,
                                                closure,
                                            ),
                                            Err(trace) => build_lookup(
                                                update_stream,
                                                trace.enter_region(region),
                                                stream_key,
                                                closure,
                                            ),
                                        }
                                    };
                                    update_stream = oks;
                                    region_errs.push(errs);
                                }

                                // We have completed the join building, but may have work remaining.
                                // For example, we may have expressions not pushed down (e.g. literals)
                                // and projections that could not be applied (e.g. column repetition).
                                if let Some(final_closure) = final_closure {
                                    let (updates, errors) = update_stream.flat_map_fallible({
                                        // Reuseable allocation for unpacking.
                                        let mut datums = DatumVec::new();
                                        move |row| {
                                            let temp_storage = RowArena::new();
                                            let mut datums_local = datums.borrow_with(&row);
                                            // TODO(mcsherry): re-use `row` allocation.
                                            final_closure
                                                .apply(&mut datums_local, &temp_storage)
                                                .map_err(DataflowError::from)
                                                .transpose()
                                        }
                                    });

                                    update_stream = updates;
                                    region_errs.push(errors);
                                }

                                inner_errs.push(
                                    differential_dataflow::collection::concatenate(
                                        region,
                                        region_errs,
                                    )
                                    .leave(),
                                );
                                update_stream.leave()
                            });

                            join_results.push(path_results);
                        }

                        scope_errs.push(
                            differential_dataflow::collection::concatenate(inner, inner_errs)
                                .leave(),
                        );

                        // Concatenate the results of each delta query as the accumulated results.
                        (
                            differential_dataflow::collection::concatenate(inner, join_results)
                                .leave(),
                            differential_dataflow::collection::concatenate(scope, scope_errs),
                        )
                    });
            results
        } else {
            panic!("render_delta_join invoked on non-delta join implementation");
        }
    }
}

use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::trace::BatchReader;
use differential_dataflow::trace::Cursor;
use differential_dataflow::trace::TraceReader;
use differential_dataflow::Collection;

/// Constructs a `lookup_map` from supplied arguments.
///
/// This method exists to factor common logic from four code paths that are generic over the type of trace.
fn build_lookup<G, Tr>(
    updates: Collection<G, Row>,
    trace: Arranged<G, Tr>,
    prev_key: Vec<MirScalarExpr>,
    closure: JoinClosure,
) -> (Collection<G, Row>, Collection<G, DataflowError>)
where
    G: Scope,
    G::Timestamp: Lattice,
    Tr: TraceReader<Time = G::Timestamp, Key = Row, Val = Row, R = isize> + Clone + 'static,
    Tr::Batch: BatchReader<Tr::Key, Tr::Val, Tr::Time, Tr::R>,
    Tr::Cursor: Cursor<Tr::Key, Tr::Val, Tr::Time, Tr::R>,
{
    let (updates, errs) = updates.map_fallible({
        // Reuseable allocation for unpacking.
        let mut datums = DatumVec::new();
        let mut row_packer = Row::default();
        move |row| {
            let temp_storage = RowArena::new();
            let datums_local = datums.borrow_with(&row);
            row_packer.clear();
            row_packer.try_extend(
                prev_key
                    .iter()
                    .map(|e| e.eval(&datums_local, &temp_storage)),
            )?;
            let row_key = row_packer.finish_and_reuse();
            // Explicit drop to release borrow on `row` so that it can be returned.
            drop(datums_local);
            Ok((row, row_key))
        }
    });

    use differential_dataflow::AsCollection;
    use timely::dataflow::operators::OkErr;

    let mut datums = DatumVec::new();
    let (oks, errs2) = dogsdogsdogs::operators::lookup_map(
        &updates,
        trace,
        move |(_row, row_key), key| {
            // Prefix key selector must populate `key` with key from prefix `row`.
            key.clone_from(&row_key);
        },
        // TODO(mcsherry): consider `RefOrMut` in `lookup` interface to allow re-use.
        move |(stream_row, _stream_row_key), diff1, lookup_row, diff2| {
            let temp_storage = RowArena::new();
            let mut datums_local = datums.borrow();
            datums_local.extend(stream_row.iter());
            datums_local.extend(lookup_row.iter());
            (
                closure.apply(&mut datums_local, &temp_storage),
                diff1 * diff2,
            )
        },
        // Three default values, for decoding keys into.
        Row::pack::<_, Datum>(None),
        Row::pack::<_, Datum>(None),
        Row::pack::<_, Datum>(None),
    )
    .inner
    .ok_err(|(x, t, d)| {
        // TODO(mcsherry): consider `ok_err()` for `Collection`.
        match x {
            Ok(x) => Ok((x, t, d)),
            Err(x) => Err((DataflowError::from(x), t, d)),
        }
    });

    (
        oks.as_collection().flat_map(|x| x),
        errs.concat(&errs2.as_collection()),
    )
}
