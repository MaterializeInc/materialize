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

use futures_util::Stream as FuturesStream;
use timely::dataflow::operators::OkErr;
use timely::dataflow::{Scope, Stream};
use timely::progress::Antichain;
use tracing::trace;

use mz_dataflow_types::{
    client::controller::storage::CollectionMetadata, sources::SourceData, DataflowError,
};
use mz_persist::location::ExternalError;
use mz_persist_client::read::ListenEvent;
use mz_repr::{Diff, Row, Timestamp};

use crate::source::SourceStatus;

/// Creates a new source that reads from a persist shard.
// TODO(aljoscha): We need to change the `shard_id` parameter to be a `Vec<ShardId>` and teach the
// operator to concurrently poll from multiple `Listen` instances. This will require us to put in
// place the code that allows selecting from multiple `Listen`s, potentially by implementing async
// `Stream` for it and then using `select_all`. And it will require us to properly combine the
// upper frontier from all shards.
pub fn persist_source<G>(
    scope: &G,
    storage_metadata: CollectionMetadata,
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
    let async_stream = async_stream::try_stream! {
       // We are reading only from worker 0. We can split the work of reading from the snapshot to
        // multiple workers, but someone has to distribute the splits. Also, in the glorious
        // STORAGE future, we will use multiple persist shards to back a STORAGE collection. Then,
        // we can read in parallel, by distributing shard reading amongst workers.
        if worker_index != 0 {
            trace!("We are not worker 0, exiting...");
            return;
        }

        let (_, read) = crate::persist_cache::open::<
                (),
                SourceData,
                mz_repr::Timestamp,
                mz_repr::Diff,
            >(
                storage_metadata.persist_location,
                storage_metadata.persist_shard,
            )
            .await
            .expect("could not open persist shard");

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
            let next = listen.next().await;
            for event in next {
                yield event;
            }
        }
    };

    let mut pinned_stream = Box::pin(async_stream);

    let (timely_stream, token) = crate::source::util::source(
        scope,
        "persist_source".to_string(),
        move |info| {
            let activator = Arc::new(scope.sync_activator_for(&info.address[..]));
            let waker = futures_util::task::waker(activator);

            // There is a bit of a mismatch: listening on a ReadHandle will give us an Antichain<T>
            // as a frontier in `Progress` messages while a timely source usually only has a single
            // `Capability` that it can downgrade. We can work around that by using a
            // `CapabilitySet`, which allows downgrading the contained capabilities to a frontier
            // but `util::source` gives us both a vanilla `Capability` and a `CapabilitySet` that
            // we have to keep downgrading because we cannot simply drop the one that we don't
            // need.
            let mut current_ts = 0;

            move |cap, cap_set, output| {
                let mut context = Context::from_waker(&waker);
                while let Poll::Ready(item) = pinned_stream.as_mut().poll_next(&mut context) {
                    match item {
                        Some(Ok(ListenEvent::Progress(upper))) => {
                            if upper.elements().is_empty() {
                                return SourceStatus::Done;
                            }
                            if let Some(first_element) = upper.first() {
                                current_ts = *first_element;
                                cap.downgrade(first_element);
                            }
                            cap_set.downgrade(upper);
                        }
                        Some(Ok(ListenEvent::Updates(mut updates))) => {
                            let cap = cap_set.delayed(&current_ts);
                            let mut session = output.session(&cap);
                            session.give_vec(&mut updates);
                        }
                        Some(Err::<_, ExternalError>(e)) => {
                            // TODO(petrosagg): error handling
                            panic!("unexpected error from persist {e}");
                        }
                        None => {
                            unreachable!("We poll from persist continuously, the Stream should therefore never be exhausted.")
                        }
                    }
                }

                SourceStatus::Alive
            }
        },
    );

    let (ok_stream, err_stream) = timely_stream.ok_err(|x| match x {
        ((Ok(()), Ok(SourceData(Ok(row)))), ts, diff) => Ok((row, ts, diff)),
        ((Ok(()), Ok(SourceData(Err(err)))), ts, diff) => Err((err, ts, diff)),
        // TODO(petrosagg): error handling
        _ => panic!("decoding failed"),
    });

    let token = Rc::new(token);

    (ok_stream, err_stream, token)
}
