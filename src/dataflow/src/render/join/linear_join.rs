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
use expr::{MapFilterProject, MirRelationExpr, MirScalarExpr};
use repr::{Datum, Row, RowArena};

use crate::operator::CollectionExt;
use crate::render::context::{Arrangement, ArrangementFlavor, ArrangementImport, Context, Diff};
use crate::render::datum_vec::DatumVec;
use crate::render::join::{JoinBuildState, JoinClosure};

// TODO(mcsherry): Identical to `DeltaPathPlan`; consider unifying.
pub struct LinearJoinPlan {
    /// The source relation from which we start the join.
    source_relation: usize,
    /// An initial closure to apply before any stages.
    ///
    /// Values of `None` indicate the identity closure.
    initial_closure: Option<JoinClosure>,
    /// A *sequence* of stages to apply one after the other.
    stage_plans: Vec<LinearStagePlan>,
    /// A concluding closure to apply after the last stage.
    ///
    /// Values of `None` indicate the identity closure.
    final_closure: Option<JoinClosure>,
}

// TODO(mcsherry): Identical to `DeltaStagePlan`; consider unifying.
pub struct LinearStagePlan {
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

impl LinearJoinPlan {
    /// Create a new join plan from the required arguments.
    pub fn create_from(
        source_relation: usize,
        equivalences: &[Vec<MirScalarExpr>],
        join_order: &[(usize, Vec<MirScalarExpr>)],
        input_mapper: expr::JoinInputMapper,
        map_filter_project: MapFilterProject,
    ) -> Self {
        // Construct initial join build state.
        // This state will evolves as we build the join dataflow.
        let mut join_build_state = JoinBuildState::new(
            input_mapper.global_columns(source_relation),
            &equivalences,
            &map_filter_project,
        );

        // We would prefer to extract a closure here, but we do not know if
        // the input will be arranged or not.
        let initial_closure = None;

        // Sequence of steps to apply.
        let mut stage_plans = Vec::with_capacity(join_order.len());

        // Track the set of bound input relations, for equivalence resolution.
        let mut bound_inputs = vec![source_relation];

        // Iterate through the join order instructions, assembling keys and
        // closures to use.
        for (lookup_relation, lookup_key) in join_order.iter() {
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
            stage_plans.push(LinearStagePlan {
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

        // Form and return the complete join plan.
        LinearJoinPlan {
            source_relation,
            initial_closure,
            stage_plans,
            final_closure,
        }
    }
}

/// Different forms the streamed data might take.
enum JoinedFlavor<G, T>
where
    G: Scope,
    G::Timestamp: Lattice + Refines<T>,
    T: Timestamp + Lattice,
{
    /// Streamed data as a collection.
    Collection(Collection<G, Row, Diff>),
    /// A dataflow-local arrangement.
    Local(Arrangement<G, Row>),
    /// An imported arrangement.
    Trace(ArrangementImport<G, Row, T>),
}

impl<G, T> Context<G, MirRelationExpr, Row, T>
where
    G: Scope,
    G::Timestamp: Lattice + Refines<T>,
    T: Timestamp + Lattice,
{
    pub fn render_join(
        &mut self,
        relation_expr: &MirRelationExpr,
        map_filter_project: MapFilterProject,
        // TODO(frank): use this argument to create a region surrounding the join.
        scope: &mut G,
    ) -> (Collection<G, Row>, Collection<G, DataflowError>) {
        if let MirRelationExpr::Join {
            inputs,
            equivalences,
            demand,
            implementation: expr::JoinImplementation::Differential((start, _start_arr), order),
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
                        dummies.push(MirScalarExpr::literal_ok(Datum::Dummy, typ.scalar_type));
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

            // Create a join plan based on the linear join.
            let linear_plan = LinearJoinPlan::create_from(
                *start,
                equivalences,
                order,
                input_mapper,
                map_filter_project,
            );

            // Collect all error streams, and concatenate them at the end.
            let mut errors = Vec::new();

            // Determine which form our maintained spine of updates will initially take.
            // First, just check out the availability of an appropriate arrangement.
            // This will be `None` in the degenerate single-input join case, which ensures
            // that we do not panic if we never go around the `stage_plans` loop.
            let arrangement = linear_plan.stage_plans.get(0).and_then(|stage| {
                self.arrangement(&inputs[linear_plan.source_relation], &stage.stream_key)
            });
            // We can use an arrangement if it exists and an initial closure does not.
            let mut joined = match (arrangement, linear_plan.initial_closure) {
                (Some(ArrangementFlavor::Local(oks, errs)), None) => {
                    errors.push(errs.as_collection(|k, _v| k.clone()));
                    JoinedFlavor::Local(oks)
                }
                (Some(ArrangementFlavor::Trace(_gid, oks, errs)), None) => {
                    errors.push(errs.as_collection(|k, _v| k.clone()));
                    JoinedFlavor::Trace(oks)
                }
                (_, initial_closure) => {
                    // TODO: extract closure from the first stage in the join plan, should it exist.
                    // TODO: apply that closure in `flat_map_ref` rather than calling `.collection`.
                    let (mut joined, errs) = self.collection(&inputs[*start]).unwrap();
                    errors.push(errs);
                    // In the current code this should always be `None`, but we have this here should
                    // we change that and want to know what we should be doing.
                    if let Some(closure) = initial_closure {
                        // If there is no starting arrangement, then we can run filters
                        // directly on the starting collection.
                        // If there is only one input, we are done joining, so run filters
                        let (j, errs) = joined.flat_map_fallible({
                            // Reuseable allocation for unpacking.
                            let mut datums = DatumVec::new();
                            move |row| {
                                let temp_storage = RowArena::new();
                                let mut datums_local = datums.borrow_with(&row);
                                // TODO(mcsherry): re-use `row` allocation.
                                closure
                                    .apply(&mut datums_local, &temp_storage)
                                    .map_err(DataflowError::from)
                                    .transpose()
                            }
                        });
                        joined = j;
                        errors.push(errs);
                    }

                    JoinedFlavor::Collection(joined)
                }
            };

            // Progress through stages, updating partial results and errors.
            for stage_plan in linear_plan.stage_plans.into_iter() {
                // Different variants of `joined` implement this differently,
                // and the logic is centralized there.
                let stream = self.differential_join(
                    joined,
                    stage_plan.stream_key,
                    &inputs[stage_plan.lookup_relation],
                    stage_plan.lookup_key,
                    stage_plan.closure,
                    &mut errors,
                );
                // Update joined results and capture any errors.
                joined = JoinedFlavor::Collection(stream);
            }

            // We have completed the join building, but may have work remaining.
            // For example, we may have expressions not pushed down (e.g. literals)
            // and projections that could not be applied (e.g. column repetition).
            if let JoinedFlavor::Collection(mut joined) = joined {
                if let Some(closure) = linear_plan.final_closure {
                    let (updates, errs) = joined.flat_map_fallible({
                        // Reuseable allocation for unpacking.
                        let mut datums = DatumVec::new();
                        move |row| {
                            let temp_storage = RowArena::new();
                            let mut datums_local = datums.borrow_with(&row);
                            // TODO(mcsherry): re-use `row` allocation.
                            closure
                                .apply(&mut datums_local, &temp_storage)
                                .map_err(DataflowError::from)
                                .transpose()
                        }
                    });

                    joined = updates;
                    errors.push(errs);
                }

                // Return joined results and all produced errors collected together.
                (
                    joined,
                    differential_dataflow::collection::concatenate(scope, errors),
                )
            } else {
                panic!("Unexpectedly arranged join output");
            }
        } else {
            panic!("render_join called on invalid expression.")
        }
    }

    /// Looks up the arrangement for the next input and joins it to the arranged
    /// version of the join of previous inputs. This is split into its own method
    /// to enable reuse of code with different types of `prev_keyed`.
    fn differential_join(
        &mut self,
        mut joined: JoinedFlavor<G, T>,
        stream_key: Vec<MirScalarExpr>,
        lookup_relation: &MirRelationExpr,
        lookup_key: Vec<MirScalarExpr>,
        closure: JoinClosure,
        errors: &mut Vec<Collection<G, DataflowError>>,
    ) -> Collection<G, Row> {
        // If we have only a streamed collection, we must first form an arrangement.
        if let JoinedFlavor::Collection(stream) = joined {
            let (keyed, errs) = stream.map_fallible({
                // Reuseable allocation for unpacking.
                let mut datums = DatumVec::new();
                move |row| {
                    let temp_storage = RowArena::new();
                    let datums_local = datums.borrow_with(&row);
                    let key = Row::try_pack(
                        stream_key
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
            errors.push(errs);
            let arranged = keyed.arrange_named::<OrdValSpine<_, _, _, _>>(&format!("JoinStage"));
            joined = JoinedFlavor::Local(arranged);
        }

        // Pre-test for the arrangement existence, so that we can populate it if the
        // collection is present but the arrangement is not.
        if self.arrangement(lookup_relation, &lookup_key[..]).is_none() {
            // The join may be faulty, and announce keys for an arrangement we have
            // not formed. This *shouldn't* happen, but we prefer to do something
            // sane rather than panic.
            // TODO: If this ever trips, the `collection` call is a wasteful way to
            // determine if we have rendered the collection.
            if self.collection(lookup_relation).is_some() {
                let arrange_by = MirRelationExpr::ArrangeBy {
                    input: Box::new(lookup_relation.clone()),
                    keys: vec![lookup_key.to_vec()],
                };
                self.render_arrangeby(&arrange_by, Some("MissingArrangement"));
            } else {
                panic!("Arrangement alarmingly absent!");
            }
        }

        // Demultiplex the four different cross products of arrangement types we might have.
        match joined {
            JoinedFlavor::Collection(_) => {
                unreachable!("JoinedFlavor::Collection variant avoided at top of method");
            }
            JoinedFlavor::Local(local) => {
                match self.arrangement(lookup_relation, &lookup_key[..]) {
                    Some(ArrangementFlavor::Local(oks, errs1)) => {
                        let (oks, errs2) = self.differential_join_inner(local, oks, closure);
                        errors.push(errs1.as_collection(|k, _v| k.clone()));
                        errors.push(errs2);
                        oks
                    }
                    Some(ArrangementFlavor::Trace(_gid, oks, errs1)) => {
                        let (oks, errs2) = self.differential_join_inner(local, oks, closure);
                        errors.push(errs1.as_collection(|k, _v| k.clone()));
                        errors.push(errs2);
                        oks
                    }
                    None => {
                        unreachable!("Arrangement absent despite explicit construction");
                    }
                }
            }
            JoinedFlavor::Trace(trace) => {
                match self.arrangement(lookup_relation, &lookup_key[..]) {
                    Some(ArrangementFlavor::Local(oks, errs1)) => {
                        let (oks, errs2) = self.differential_join_inner(trace, oks, closure);
                        errors.push(errs1.as_collection(|k, _v| k.clone()));
                        errors.push(errs2);
                        oks
                    }
                    Some(ArrangementFlavor::Trace(_gid, oks, errs1)) => {
                        let (oks, errs2) = self.differential_join_inner(trace, oks, closure);
                        errors.push(errs1.as_collection(|k, _v| k.clone()));
                        errors.push(errs2);
                        oks
                    }
                    None => {
                        unreachable!("Arrangement absent despite explicit construction");
                    }
                }
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
        closure: JoinClosure,
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
        let (oks, err) = prev_keyed
            .join_core(&next_input, move |_keys, old, new| {
                let temp_storage = RowArena::new();
                let mut datums_local = datums.borrow();
                datums_local.extend(old.iter());
                datums_local.extend(new.iter());

                closure
                    .apply(&mut datums_local, &temp_storage)
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
