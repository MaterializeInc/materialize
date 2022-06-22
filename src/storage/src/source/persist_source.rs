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
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;

use futures_util::Stream as FuturesStream;
use mz_persist_client::cache::PersistClientCache;
use timely::dataflow::operators::OkErr;
use timely::dataflow::{Scope, Stream};
use timely::progress::Antichain;
use tokio::sync::Mutex;
use tracing::trace;

use mz_dataflow_types::{
    client::controller::storage::CollectionMetadata, sources::SourceData, DataflowError,
};
use mz_persist::location::ExternalError;
use mz_persist_client::read::ListenEvent;
use mz_repr::{Diff, Row, Timestamp};

use crate::source::{SourceStatus, YIELD_INTERVAL};

/// Creates a new source that reads from a persist shard.
///
/// All times emitted will have been [advanced by] the given `as_of` frontier.
///
/// [advanced by]: differential_dataflow::lattice::Lattice::advance_by
//
// TODO(aljoscha): We need to change the `shard_id` parameter to be a `Vec<ShardId>` and teach the
// operator to concurrently poll from multiple `Listen` instances. This will require us to put in
// place the code that allows selecting from multiple `Listen`s, potentially by implementing async
// `Stream` for it and then using `select_all`. And it will require us to properly combine the
// upper frontier from all shards.
pub fn persist_source<G>(
    scope: &G,
    storage_metadata: CollectionMetadata,
    persist_clients: Arc<Mutex<PersistClientCache>>,
    as_of: Antichain<Timestamp>,
) -> (
    Stream<G, (Row, Timestamp, Diff)>,
    Stream<G, (DataflowError, Timestamp, Diff)>,
    Rc<dyn Any>,
)
where
    G: Scope<Timestamp = mz_repr::Timestamp>,
{
    let worker_index = scope.index();

    // This source is split into two parts: a first part that sets up `async_stream` and a timely
    // source operator that the continuously reads from that stream.
    //
    // It is split that way because there is currently no easy way of setting up an async source
    // operator in materialize/timely.

    // This is a generator that sets up an async `Stream` that can be continously polled to get the
    // values that are `yield`-ed from it's body.
    let async_stream = async_stream::try_stream!({
        // We are reading only from worker 0. We can split the work of reading from the snapshot to
        // multiple workers, but someone has to distribute the splits. Also, in the glorious
        // STORAGE future, we will use multiple persist shards to back a STORAGE collection. Then,
        // we can read in parallel, by distributing shard reading amongst workers.
        if worker_index != 0 {
            trace!("We are not worker 0, exiting...");
            return;
        }

        let mut read = persist_clients
            .lock()
            .await
            .open(storage_metadata.persist_location)
            .await
            .expect("could not open persist client")
            .open_reader::<SourceData, (), mz_repr::Timestamp, mz_repr::Diff>(
                storage_metadata.persist_shard,
            )
            .await
            .expect("could not open persist shard");

        /// Aggressively downgrade `since`, to not hold back compaction.
        read.downgrade_since(as_of.clone()).await;

        let mut snapshot_iter = read
            .snapshot(as_of.clone())
            .await
            .expect("cannot serve requested as_of");

        // First, yield all the updates from the snapshot.
        while let Some(next) = snapshot_iter.next().await {
            yield ListenEvent::Updates(next);
        }

        // Then, listen continously and yield any new updates. This loop is expected to never
        // finish.
        let mut listen = read
            .listen(as_of)
            .await
            .expect("cannot serve requested as_of");

        loop {
            for event in listen.next().await {
                // TODO(petrosagg): We are incorrectly NOT downgrading the since frontier of this
                // read handle which will hold back compaction in persist. This is currently a
                // necessary evil to avoid too much contension on persist's consensus
                // implementation.
                //
                // Once persist supports compaction and/or has better performance the code below
                // should be enabled.
                // if let ListenEvent::Progress(upper) = &event {
                //     read.downgrade_since(upper.clone()).await;
                // }
                yield event;
            }
        }
    });

    let mut pinned_stream = Box::pin(async_stream);

    let (timely_stream, token) =
        crate::source::util::source(scope, "persist_source".to_string(), move |info| {
            let waker_activator = Arc::new(scope.sync_activator_for(&info.address[..]));
            let waker = futures_util::task::waker(waker_activator);
            let activator = scope.activator_for(&info.address[..]);

            // There is a bit of a mismatch: listening on a ReadHandle will give us an Antichain<T>
            // as a frontier in `Progress` messages while a timely source usually only has a single
            // `Capability` that it can downgrade. We can work around that by using a
            // `CapabilitySet`, which allows downgrading the contained capabilities to a frontier
            // but `util::source` gives us both a vanilla `Capability` and a `CapabilitySet` that
            // we have to keep downgrading because we cannot simply drop the one that we don't
            // need.
            let mut current_ts = 0;

            move |cap, output| {
                let mut context = Context::from_waker(&waker);
                // Bound execution of operator to prevent a single operator from hogging
                // the CPU if there are many messages to process
                let timer = Instant::now();

                while let Poll::Ready(item) = pinned_stream.as_mut().poll_next(&mut context) {
                    match item {
                        Some(Ok(ListenEvent::Progress(upper))) => match upper.into_option() {
                            Some(ts) => {
                                current_ts = ts;
                                cap.downgrade(&ts);
                            }
                            None => return SourceStatus::Done,
                        },
                        Some(Ok(ListenEvent::Updates(mut updates))) => {
                            // This operator guarantees that its output has been advanced by `as_of.
                            // The persist SnapshotIter already has this contract, so nothing to do
                            // here.
                            let cap = cap.delayed(&current_ts);
                            let mut session = output.session(&cap);
                            session.give_vec(&mut updates);
                        }
                        Some(Err::<_, ExternalError>(e)) => {
                            // TODO(petrosagg): error handling
                            panic!("unexpected error from persist {e}");
                        }
                        None => return SourceStatus::Done,
                    }
                    if timer.elapsed() > YIELD_INTERVAL {
                        activator.activate();
                        break;
                    }
                }

                SourceStatus::Alive
            }
        });

    let (ok_stream, err_stream) = timely_stream.ok_err(|x| match x {
        ((Ok(SourceData(Ok(row))), Ok(())), ts, diff) => Ok((row, ts, diff)),
        ((Ok(SourceData(Err(err))), Ok(())), ts, diff) => Err((err, ts, diff)),
        // TODO(petrosagg): error handling
        _ => panic!("decoding failed"),
    });

    let token = Rc::new(token);

    (ok_stream, err_stream, token)
}
