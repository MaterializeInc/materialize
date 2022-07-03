// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Rendering of linear join plans.
//!
//! Consult [LinearJoinPlan] documentation for details.

use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::arrangement::Arrange;
use differential_dataflow::operators::arrange::arrangement::Arranged;
use differential_dataflow::operators::join::JoinCore;
use differential_dataflow::trace::TraceReader;
use differential_dataflow::Collection;
use timely::dataflow::Scope;
use timely::progress::{timestamp::Refines, Timestamp};

use mz_compute_client::plan::join::linear_join::{LinearJoinPlan, LinearStagePlan};
use mz_compute_client::plan::join::JoinClosure;
use mz_repr::{DatumVec, Diff, Row, RowArena};
use mz_storage::client::errors::DataflowError;
use mz_timely_util::operator::CollectionExt;

use crate::render::context::{
    Arrangement, ArrangementFlavor, ArrangementImport, CollectionBundle, Context,
};
use crate::typedefs::RowSpine;

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

impl<G, T> Context<G, Row, T>
where
    G: Scope,
    G::Timestamp: Lattice + Refines<T>,
    T: Timestamp + Lattice,
{
    pub(crate) fn render_join(
        &mut self,
        inputs: Vec<CollectionBundle<G, Row, T>>,
        linear_plan: LinearJoinPlan,
        scope: &mut G,
    ) -> CollectionBundle<G, Row, T> {
        // Collect all error streams, and concatenate them at the end.
        let mut errors = Vec::new();

        // Determine which form our maintained spine of updates will initially take.
        // First, just check out the availability of an appropriate arrangement.
        // This will be `None` in the degenerate single-input join case, which ensures
        // that we do not panic if we never go around the `stage_plans` loop.
        let arrangement = linear_plan
            .stage_plans
            .get(0)
            .and_then(|stage| inputs[linear_plan.source_relation].arrangement(&stage.stream_key));
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
                let (mut joined, errs) = inputs[linear_plan.source_relation]
                    .as_specific_collection(linear_plan.source_key.as_deref());
                errors.push(errs);

                // In the current code this should always be `None`, but we have this here should
                // we change that and want to know what we should be doing.
                if let Some(closure) = initial_closure {
                    // If there is no starting arrangement, then we can run filters
                    // directly on the starting collection.
                    // If there is only one input, we are done joining, so run filters
                    let (j, errs) = joined.flat_map_fallible("LinearJoinInitialization", {
                        // Reuseable allocation for unpacking.
                        let mut datums = DatumVec::new();
                        let mut row_builder = Row::default();
                        move |row| {
                            let temp_storage = RowArena::new();
                            let mut datums_local = datums.borrow_with(&row);
                            // TODO(mcsherry): re-use `row` allocation.
                            closure
                                .apply(&mut datums_local, &temp_storage, &mut row_builder)
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

        // progress through stages, updating partial results and errors.
        for stage_plan in linear_plan.stage_plans.into_iter() {
            // Different variants of `joined` implement this differently,
            // and the logic is centralized there.
            let stream = self.differential_join(
                joined,
                inputs[stage_plan.lookup_relation].clone(),
                stage_plan,
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
                let (updates, errs) = joined.flat_map_fallible("LinearJoinFinalization", {
                    // Reuseable allocation for unpacking.
                    let mut datums = DatumVec::new();
                    let mut row_builder = Row::default();
                    move |row| {
                        let temp_storage = RowArena::new();
                        let mut datums_local = datums.borrow_with(&row);
                        // TODO(mcsherry): re-use `row` allocation.
                        closure
                            .apply(&mut datums_local, &temp_storage, &mut row_builder)
                            .map_err(DataflowError::from)
                            .transpose()
                    }
                });

                joined = updates;
                errors.push(errs);
            }

            // Return joined results and all produced errors collected together.
            CollectionBundle::from_collections(
                joined,
                differential_dataflow::collection::concatenate(scope, errors),
            )
        } else {
            panic!("Unexpectedly arranged join output");
        }
    }

    /// Looks up the arrangement for the next input and joins it to the arranged
    /// version of the join of previous inputs. This is split into its own method
    /// to enable reuse of code with different types of `prev_keyed`.
    fn differential_join(
        &mut self,
        mut joined: JoinedFlavor<G, T>,
        lookup_relation: CollectionBundle<G, Row, T>,
        LinearStagePlan {
            stream_key,
            stream_thinning,
            lookup_key,
            closure,
            lookup_relation: _,
        }: LinearStagePlan,
        errors: &mut Vec<Collection<G, DataflowError, Diff>>,
    ) -> Collection<G, Row, Diff> {
        // If we have only a streamed collection, we must first form an arrangement.
        if let JoinedFlavor::Collection(stream) = joined {
            let mut row_buf = Row::default();
            let (keyed, errs) = stream.map_fallible("LinearJoinKeyPreparation", {
                // Reuseable allocation for unpacking.
                let mut datums = DatumVec::new();
                move |row| {
                    let temp_storage = RowArena::new();
                    let datums_local = datums.borrow_with(&row);
                    row_buf.packer().try_extend(
                        stream_key
                            .iter()
                            .map(|e| e.eval(&datums_local, &temp_storage)),
                    )?;
                    let key = row_buf.clone();
                    row_buf
                        .packer()
                        .extend(stream_thinning.iter().map(|e| datums_local[*e]));
                    let value = row_buf.clone();
                    Ok((key, value))
                }
            });

            errors.push(errs);
            let arranged = keyed.arrange_named::<RowSpine<_, _, _, _>>(&format!("JoinStage"));
            joined = JoinedFlavor::Local(arranged);
        }

        // Demultiplex the four different cross products of arrangement types we might have.
        let arrangement = lookup_relation
            .arrangement(&lookup_key[..])
            .expect("Arrangement absent despite explicit construction");
        match joined {
            JoinedFlavor::Collection(_) => {
                unreachable!("JoinedFlavor::Collection variant avoided at top of method");
            }
            JoinedFlavor::Local(local) => match arrangement {
                ArrangementFlavor::Local(oks, errs1) => {
                    let (oks, errs2) = self.differential_join_inner(local, oks, closure);
                    errors.push(errs1.as_collection(|k, _v| k.clone()));
                    errors.push(errs2);
                    oks
                }
                ArrangementFlavor::Trace(_gid, oks, errs1) => {
                    let (oks, errs2) = self.differential_join_inner(local, oks, closure);
                    errors.push(errs1.as_collection(|k, _v| k.clone()));
                    errors.push(errs2);
                    oks
                }
            },
            JoinedFlavor::Trace(trace) => match arrangement {
                ArrangementFlavor::Local(oks, errs1) => {
                    let (oks, errs2) = self.differential_join_inner(trace, oks, closure);
                    errors.push(errs1.as_collection(|k, _v| k.clone()));
                    errors.push(errs2);
                    oks
                }
                ArrangementFlavor::Trace(_gid, oks, errs1) => {
                    let (oks, errs2) = self.differential_join_inner(trace, oks, closure);
                    errors.push(errs1.as_collection(|k, _v| k.clone()));
                    errors.push(errs2);
                    oks
                }
            },
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
    ) -> (Collection<G, Row, Diff>, Collection<G, DataflowError, Diff>)
    where
        J: JoinCore<G, Row, Row, mz_repr::Diff>,
        Tr2: TraceReader<Key = Row, Val = Row, Time = G::Timestamp, R = mz_repr::Diff>
            + Clone
            + 'static,
    {
        use differential_dataflow::AsCollection;
        use timely::dataflow::operators::OkErr;

        // Reuseable allocation for unpacking.
        let mut datums = DatumVec::new();
        let mut row_builder = Row::default();

        let (oks, err) = prev_keyed
            .join_core(&next_input, move |key, old, new| {
                let temp_storage = RowArena::new();
                let mut datums_local = datums.borrow_with_many(&[key, old, new]);
                closure
                    .apply(&mut datums_local, &temp_storage, &mut row_builder)
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
