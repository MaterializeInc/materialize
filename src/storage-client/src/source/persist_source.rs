// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A source that reads from an a persist shard.

use std::any::Any;

use std::cmp::{Ordering, Reverse};

use std::collections::{BTreeMap, BinaryHeap};
use std::rc::Rc;
use std::sync::Arc;
use std::time::Instant;

use differential_dataflow::AsCollection;
use timely::communication::Push;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::channels::Bundle;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::{Capability, Enter, Inspect, Map, OkErr, Operator};
use timely::dataflow::{Scope, Stream};
use timely::progress::timestamp::Refines;
use timely::progress::{Antichain, Timestamp as TimelyTimestamp};
use timely::scheduling::Activator;

use mz_expr::MfpPlan;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::fetch::FetchedPart;
use mz_persist_client::operators::shard_source::shard_source;
pub use mz_persist_client::operators::shard_source::FlowControl;
use mz_persist_types::codec_impls::UnitSchema;
use mz_repr::{DatumVec, Diff, GlobalId, Row, Timestamp};
use mz_timely_util::buffer::ConsolidateBuffer;
use mz_timely_util::order::Hybrid;

use crate::controller::CollectionMetadata;
use crate::source::util::{Prefixed, Sort, WithPrefix};
use crate::types::errors::DataflowError;
use crate::types::sources::SourceData;

type HybridT = Hybrid<Timestamp, Sort>;

/// Creates a new source that reads from a persist shard, distributing the work
/// of reading data to all timely workers.
///
/// All times emitted will have been [advanced by] the given `as_of` frontier.
/// All updates at times greater or equal to `until` will be suppressed.
/// The `map_filter_project` argument, if supplied, may be partially applied,
/// and any un-applied part of the argument will be left behind in the argument.
///
/// Users of this function have the ability to apply flow control to the output
/// to limit the in-flight data (measured in bytes) it can emit. The flow control
/// input is a timely stream that communicates the frontier at which the data
/// emitted from by this source have been dropped.
///
/// **Note:** Because this function is reading batches from `persist`, it is working
/// at batch granularity. In practice, the source will be overshooting the target
/// flow control upper by an amount that is related to the size of batches.
///
/// If no flow control is desired an empty stream whose frontier immediately advances
/// to the empty antichain can be used. An easy easy of creating such stream is by
/// using [`timely::dataflow::operators::generic::operator::empty`].
///
/// [advanced by]: differential_dataflow::lattice::Lattice::advance_by
pub fn persist_source<G, YFn>(
    scope: &mut G,
    source_id: GlobalId,
    persist_clients: Arc<PersistClientCache>,
    metadata: CollectionMetadata,
    as_of: Option<Antichain<Timestamp>>,
    until: Antichain<Timestamp>,
    map_filter_project: Option<&mut MfpPlan>,
    flow_control: Option<FlowControl<G>>,
    yield_fn: YFn,
) -> (
    Stream<G, (Row, Timestamp, Diff)>,
    Stream<G, (DataflowError, Timestamp, Diff)>,
    Rc<dyn Any>,
)
where
    G: Scope<Timestamp = mz_repr::Timestamp>,
    YFn: Fn(Instant, usize) -> bool + 'static,
{
    let (stream, token) = persist_source_core(
        scope,
        source_id,
        persist_clients,
        metadata,
        as_of,
        until,
        map_filter_project,
        flow_control,
        yield_fn,
    );
    let (ok_stream, err_stream) = stream.ok_err(|(d, t, r)| match d {
        Ok(row) => Ok((row, t, r)),
        Err(err) => Err((err, t, r)),
    });
    (ok_stream, err_stream, token)
}

/// Creates a new source that reads from a persist shard, distributing the work
/// of reading data to all timely workers.
///
/// All times emitted will have been [advanced by] the given `as_of` frontier.
///
/// [advanced by]: differential_dataflow::lattice::Lattice::advance_by
#[allow(clippy::needless_borrow)]
pub fn persist_source_core<G, YFn>(
    scope: &mut G,
    source_id: GlobalId,
    persist_clients: Arc<PersistClientCache>,
    metadata: CollectionMetadata,
    as_of: Option<Antichain<Timestamp>>,
    until: Antichain<Timestamp>,
    map_filter_project: Option<&mut MfpPlan>,
    flow_control: Option<FlowControl<G>>,
    yield_fn: YFn,
) -> (
    Stream<G, (Result<Row, DataflowError>, Timestamp, Diff)>,
    Rc<dyn Any>,
)
where
    G: Scope<Timestamp = mz_repr::Timestamp>,
    YFn: Fn(Instant, usize) -> bool + 'static,
{
    let name = source_id.to_string();
    let (fetched, token) = shard_source(
        scope,
        &name,
        persist_clients,
        metadata.persist_location,
        metadata.data_shard,
        as_of,
        until.clone(),
        flow_control,
        Arc::new(Prefixed(Arc::new(metadata.relation_desc))),
        Arc::new(UnitSchema),
    );

    // TODO: this becomes a parameter!
    let consolidate = false;

    let rows = if consolidate {
        scope.scoped::<HybridT, _, _>("with_hybrid_timestamp", |scope| {
            let rows = decode_and_mfp(
                &fetched.enter(scope),
                &name,
                until,
                map_filter_project,
                yield_fn,
            );
            // A slightly awkward consolidation dance. We timestamp each record with the timestamp of
            // the part they're in, keeping the actual timestamp along with the data; then after consolidating
            // we'll swap the timestamp back into its normal position. This ensures b
            let rows = rows.unary(Pipeline, "batch_time", |_, _| {
                let mut vector = Vec::new();
                move |input, output| {
                    input.for_each(|cap, data| {
                        data.swap(&mut vector);
                        output.session(&cap).give_iterator(vector.drain(..).map(
                            |(result, data_time, diff)| {
                                let Hybrid(data_ts, data_sort) = data_time;
                                let batch_time = Hybrid(cap.time().0, data_sort);
                                ((result, data_ts), batch_time, diff)
                            },
                        ));
                    })
                }
            });
            rows.as_collection()
                .consolidate() // TODO: use the proper spine type
                // .inner
                // .inspect_core({
                //     // The main reason to consolidate in the inner scope is so that we don't have
                //     // to buffer all the records with the same mztime, which means that we should
                //     // see the consolidate start to emit data before the time ticks over.
                //     let mut time_map = BTreeMap::new();
                //     move |data_or_frontier| match data_or_frontier {
                //         Ok((time, records)) => {
                //             let count = time_map.entry(time.clone()).or_insert(0);
                //             *count += records.len();
                //         }
                //         Err(frontier) => {
                //             let antichain = AntichainRef::new(frontier);
                //             let mut optimizable_count = 0;
                //             while let Some((k, v)) = time_map.first_key_value() {
                //                 if antichain.less_equal(k) {
                //                     // Stop discarding the counts once we're up to the frontier.
                //                     break;
                //                 }
                //                 if let Some(at) = antichain.as_option() {
                //                     if k.0 == at.0 {
                //                         // Found some records that were emitted early within the current
                //                         // frontier.
                //                         optimizable_count += v;
                //                     }
                //                 }
                //                 time_map.pop_first();
                //             }
                //             if optimizable_count > 0 {
                //                 info!("Good news: emitted {optimizable_count} records before time advanced to {frontier:?}");
                //             }
                //         }
                //     }
                // }).as_collection()
                .leave()
                .inner
                .map(|((r, t), _, d)| (r, t, d))
        })
    } else {
        decode_and_mfp(&fetched, &name, until, map_filter_project, yield_fn)
    };

    (rows, token)
}

pub fn decode_and_mfp<G, YFn>(
    fetched: &Stream<G, FetchedPart<WithPrefix<SourceData>, (), Timestamp, Diff>>,
    name: &str,
    until: Antichain<Timestamp>,
    mut map_filter_project: Option<&mut MfpPlan>,
    yield_fn: YFn,
) -> Stream<G, (Result<Row, DataflowError>, G::Timestamp, Diff)>
where
    G: Scope,
    HybridT: Refines<G::Timestamp>,
    YFn: Fn(Instant, usize) -> bool + 'static,
{
    let scope = fetched.scope();
    let mut builder = OperatorBuilder::new(
        format!("persist_source::decode_and_mfp({})", name),
        scope.clone(),
    );
    let operator_info = builder.operator_info();

    let mut fetched_input = builder.new_input(fetched, Pipeline);
    let (mut updates_output, updates_stream) = builder.new_output();

    // Re-used state for processing and building rows.
    let mut datum_vec = mz_repr::DatumVec::new();
    let mut row_builder = Row::default();

    // Extract the MFP if it exists; leave behind an identity MFP in that case.
    let map_filter_project = map_filter_project.as_mut().map(|mfp| mfp.take());

    builder.build(move |_caps| {
        // Acquire an activator to reschedule the operator when it has unfinished work.
        let activations = scope.activations();
        let activator = Activator::new(&operator_info.address[..], activations);
        // Maintain a list of work to do
        let mut pending_work = WorkQueue {
            next_id: 0,
            work: Default::default(),
            queue: Default::default(),
        };
        let mut buffer = Default::default();

        move |_frontier| {
            fetched_input.for_each(|time, data| {
                data.swap(&mut buffer);
                let capability = time.retain();
                for fetched_part in buffer.drain(..) {
                    pending_work.push(PendingWork {
                        capability: capability.clone(),
                        fetched_part,
                    })
                }
            });

            let mut work = 0;
            let start_time = Instant::now();
            let mut output = updates_output.activate();
            let mut handle = ConsolidateBuffer::new(&mut output, 0);
            let done = pending_work.do_work(
                &mut work,
                start_time,
                &yield_fn,
                &until,
                map_filter_project.as_ref(),
                &mut datum_vec,
                &mut row_builder,
                &mut handle,
            );
            if done {
                assert!(pending_work.work.is_empty());
                assert!(pending_work.queue.is_empty());
            } else {
                activator.activate();
            }
        }
    });

    updates_stream
}

struct WorkQueue<T: TimelyTimestamp> {
    next_id: usize,
    work: BTreeMap<usize, PendingWork<T>>,
    queue: BinaryHeap<Reverse<(HybridT, usize)>>,
}

impl<T> WorkQueue<T>
where
    T: TimelyTimestamp,
    HybridT: Refines<T>,
{
    fn push(&mut self, work: PendingWork<T>) {
        let id = self.next_id;
        self.next_id += 1;
        self.queue.push(Reverse((
            HybridT::to_inner(work.capability.time().clone()),
            id,
        )));
        let removed = self.work.insert(id, work);
        assert!(removed.is_none());
    }

    /// Perform work, reading from the fetched part, decoding, and sending outputs, while checking
    /// `yield_fn` whether more fuel is available.
    fn do_work<P, YFn>(
        &mut self,
        work: &mut usize,
        start_time: Instant,
        yield_fn: YFn,
        until: &Antichain<Timestamp>,
        map_filter_project: Option<&MfpPlan>,
        datum_vec: &mut DatumVec,
        row_builder: &mut Row,
        output: &mut ConsolidateBuffer<T, Result<Row, DataflowError>, Diff, P>,
    ) -> bool
    where
        P: Push<Bundle<T, (Result<Row, DataflowError>, T, Diff)>>,
        YFn: Fn(Instant, usize) -> bool,
    {
        'work_loop: while let Some(Reverse((mut time_sort, id))) = self.queue.pop() {
            let front = self.work.get_mut(&id).expect("retrieving known id");
            let is_probably_ordered =
                front.fetched_part.desc().since() != &Antichain::from_elem(Timestamp::minimum());
            while let Some(((key, val), time, diff)) = front.fetched_part.next() {
                if until.less_equal(&time) {
                    continue;
                }
                // TODO(petrosagg): error handling
                let WithPrefix(key, sort) = key.expect("decoded key");
                let () = val.expect("decoded val");

                let sort = Sort::Data(sort);
                if is_probably_ordered {
                    // The sort of the data matches the sort of the file.
                    match time_sort.1.cmp(&sort) {
                        Ordering::Less => {
                            time_sort.1 = sort.clone();
                        }
                        Ordering::Equal => {}
                        Ordering::Greater => {
                            panic!("unexpectedly out-of-order timestamps");
                        }
                    }
                }

                match key {
                    SourceData(Ok(row)) => {
                        if let Some(mfp) = map_filter_project {
                            let arena = mz_repr::RowArena::new();
                            let mut datums_local = datum_vec.borrow_with(&row);
                            for result in mfp.evaluate(
                                &mut datums_local,
                                &arena,
                                time,
                                diff,
                                |time| !until.less_equal(time),
                                row_builder,
                            ) {
                                match result {
                                    Ok((row, time, diff)) => {
                                        // Additional `until` filtering due to temporal filters.
                                        if !until.less_equal(&time) {
                                            let time = Hybrid(time, sort.clone()).to_outer();
                                            output
                                                .give_at(&front.capability, (Ok(row), time, diff));
                                            *work += 1;
                                        }
                                    }
                                    Err((err, time, diff)) => {
                                        // Additional `until` filtering due to temporal filters.
                                        if !until.less_equal(&time) {
                                            let time = Hybrid(time, sort.clone()).to_outer();
                                            output
                                                .give_at(&front.capability, (Err(err), time, diff));
                                            *work += 1;
                                        }
                                    }
                                }
                            }
                        } else {
                            let time = Hybrid(time, sort).to_outer();
                            output.give_at(&front.capability, (Ok(row), time, diff));
                            *work += 1;
                        }
                    }
                    SourceData(Err(err)) => {
                        let time = Hybrid(time, sort).to_outer();
                        output.give_at(&front.capability, (Err(err), time, diff));
                        *work += 1;
                    }
                }

                if let Some(Reverse((next_best, _))) = self.queue.peek() {
                    if time_sort > *next_best {
                        front.capability.downgrade(&time_sort.clone().to_outer());
                        self.queue.push(Reverse((time_sort, id)));
                        continue 'work_loop;
                    }
                }

                if yield_fn(start_time, *work) {
                    front.capability.downgrade(&time_sort.clone().to_outer());
                    self.queue.push(Reverse((time_sort, id)));
                    return false;
                }
            }

            let removed = self.work.remove(&id);
            assert!(removed.is_some(), "removing completed work");
        }

        true
    }
}

/// Pending work to read from fetched parts
struct PendingWork<T: TimelyTimestamp> {
    /// The time at which the work should happen.
    capability: Capability<T>,
    /// Pending fetched part.
    fetched_part: FetchedPart<WithPrefix<SourceData>, (), Timestamp, Diff>,
}
