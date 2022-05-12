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
use timely::dataflow::operators::{Concat, Map, OkErr};
use timely::dataflow::{Scope, Stream};
use timely::progress::Antichain;
use tracing::trace;

use mz_dataflow_types::{DataflowError, DecodeError, SourceError, SourceErrorDetails};
use mz_persist_client::read::ListenEvent;
use mz_persist_client::{PersistLocation, ShardId};
use mz_repr::{Diff, GlobalId, Row, Timestamp};

use crate::source::SourceStatus;
use crate::source::YIELD_INTERVAL;

/// Creates a new source that reads from a persist shard.
// TODO(aljoscha): We need to change the `shard_id` parameter to be a `Vec<ShardId>` and teach the
// operator to concurrently poll from multiple `Listen` instances. This will require us to put in
// place the code that allows selecting from multiple `Listen`s, potentially by implementing async
// `Stream` for it and then using `select_all`. And it will require us to properly combine the
// upper frontier from all shards.
pub fn persist_source<G>(
    scope: &G,
    source_id: GlobalId,
    consensus_uri: String,
    blob_uri: String,
    shard_id: ShardId,
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

        let persist_location = PersistLocation {
            consensus_uri: consensus_uri,
            blob_uri: blob_uri,
        };

        let persist_client = persist_location.open().await?;

        let (_write, mut read) = persist_client
            .open::<Row, Row, Timestamp, Diff>(shard_id)
            .await?;

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
            let next = listen.next().await;
            for event in next {
                // TODO(aljoscha): We should introduce a method that consumes a `ReadHandle` and
                // returns a `Listen` that automatically downgrades `since` as early as possible.
                if let ListenEvent::Progress(upper) = &event {
                    read.downgrade_since(upper.clone()).await;
                }
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

            // There is a bit of a mismatch: listening on a ReadHandle will give us an Antichain<T>
            // as a frontier in `Progress` messages while a timely source usually only has a single
            // `Capability` that it can downgrade. We can work around that by using a
            // `CapabilitySet`, which allows downgrading the contained capabilities to a frontier
            // but `util::source` gives us both a vanilla `Capability` and a `CapabilitySet` that
            // we have to keep downgrading because we cannot simply drop the one that we don't
            // need.
            let mut current_ts = 0;

            move |cap, cap_set, output| {
                let waker = futures_util::task::waker_ref(&activator);
                let mut context = Context::from_waker(&waker);

                // Bound execution of operator to prevent a single operator from hogging
                // the CPU if there are many messages to process
                let timer = Instant::now();

                while let Poll::Ready(item) = pinned_stream.as_mut().poll_next(&mut context) {
                    match item {
                        Some(Ok(event)) => match event {
                            ListenEvent::Progress(upper) => {
                                // NOTE(aljoscha): Ideally, we would only get a CapabilitySet, that
                                // we have to manage. Or the ability to drop the plain Capability
                                // right at the start.
                                if let Some(first_element) = upper.first() {
                                    current_ts = *first_element;
                                    cap.downgrade(first_element);
                                }
                                cap_set.downgrade(upper);
                            }
                            ListenEvent::Updates(updates) => {
                                let cap = cap_set.delayed(&current_ts);
                                let mut session = output.session(&cap);
                                for update in updates {
                                    session.give(Ok(update));
                                }
                            }
                        },
                        Some(Err::<_, anyhow::Error>(e)) => {
                            let cap = cap_set.delayed(&current_ts);
                            let mut session = output.session(&cap);
                            session.give(Err((format!("{}", e), current_ts, 1)));
                        }
                        None => {
                            unreachable!("We poll from persist continuously, the Stream should therefore never be exhausted.")
                        }
                    }

                    if timer.elapsed() > YIELD_INTERVAL {
                        let _ = activator.activate();
                        return SourceStatus::Alive;
                    }
                }

                SourceStatus::Alive
            }
        },
    );

    let (ok_stream, persist_err_stream) = timely_stream.ok_err(|x| x);
    let persist_err_stream = persist_err_stream.map(move |(err, ts, diff)| {
        (
            DataflowError::from(SourceError::new(
                source_id,
                SourceErrorDetails::Persistence(err),
            )),
            ts,
            diff,
        )
    });

    let mut row = Row::default();
    let (ok_stream, err_stream) = ok_stream.ok_err(move |((key, value), ts, diff)| {
        let mut row_packer = row.packer();

        let key = match key {
            Ok(key) => key,
            Err(e) => return Err((DataflowError::from(DecodeError::Text(e)), ts, diff)),
        };
        let value = match value {
            Ok(value) => value,
            Err(e) => return Err((DataflowError::from(DecodeError::Text(e)), ts, diff)),
        };

        let unpacked_key = key.unpack();
        let unpacked_value = value.unpack();
        row_packer.extend(unpacked_key);
        row_packer.extend(unpacked_value);

        // TODO(aljoscha): Metrics about how many messages have been ingested from persist/STORAGE?
        // metrics.messages_ingested.inc();
        Ok((row.clone(), ts, diff))
    });

    let err_stream = err_stream.concat(&persist_err_stream);

    let token = Rc::new(token);

    (ok_stream, err_stream, token)
}
