// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A Timely Dataflow operator that emits the records in a snapshot.

use persist_types::Codec;
use timely::dataflow::operators::generic::operator;
use timely::dataflow::operators::Map;
use timely::dataflow::{Scope, Stream};
use timely::Data as TimelyData;

use crate::error::Error;
use crate::indexed::runtime::DecodedSnapshot;
use crate::indexed::Snapshot;

/// Extension trait for [`Stream`].
pub trait Replay<G: Scope<Timestamp = u64>, K: TimelyData, V: TimelyData> {
    /// Emits each record in a snapshot.
    fn replay(
        &self,
        snapshot: Result<DecodedSnapshot<K, V>, Error>,
    ) -> Stream<G, (Result<(K, V), String>, u64, isize)>;
}

impl<G, K, V> Replay<G, K, V> for G
where
    G: Scope<Timestamp = u64>,
    K: TimelyData + Codec,
    V: TimelyData + Codec,
{
    fn replay(
        &self,
        snapshot: Result<DecodedSnapshot<K, V>, Error>,
    ) -> Stream<G, (Result<(K, V), String>, u64, isize)> {
        // TODO: This currently works by only emitting the persisted
        // data on worker 0 because that was the simplest thing to do
        // initially. Instead, we should shard up the responsibility
        // between all the workers.
        let active_worker = self.index() == 0;

        let result_stream: Stream<G, Result<((K, V), u64, isize), Error>> =
            operator::source(self, "Replay", move |cap, _info| {
                let mut snapshot_cap = if active_worker {
                    Some((snapshot, cap))
                } else {
                    None
                };

                move |output| {
                    let (snapshot, cap) = match snapshot_cap.take() {
                        Some(x) => x,
                        None => return, // We were already invoked and consumed our snapshot.
                    };

                    let mut session = output.session(&cap);

                    match snapshot {
                        Ok(snapshot) => {
                            let snapshot_since = snapshot.since();
                            // TODO: Periodically yield to let the rest of the dataflow
                            // reduce this down.
                            for x in snapshot.into_iter() {
                                if let Ok((_, ts, _)) = &x {
                                    // The raw update data held internally in the
                                    // snapshot may not be physically compacted up to
                                    // the logical compaction frontier of since.
                                    // Snapshot handles advancing any necessary data but
                                    // we double check that invariant here.
                                    debug_assert!(snapshot_since.less_equal(ts));
                                }
                                session.give(x);
                            }
                        }
                        Err(e) => {
                            session.give(Err(Error::String(format!(
                                "replaying persisted data: {}",
                                e
                            ))));
                        }
                    }
                }
            });
        result_stream.map(|x| {
            match x {
                Ok((kv, ts, diff)) => (Ok(kv), ts, diff),
                Err(err) => {
                    // TODO: Make the responsibility for retries in the presence
                    // of transient storage failures lie with the snapshot (with
                    // appropriate monitoring). At the limit, we should be able
                    // to retry even the compaction+deletion of a batch that we
                    // were supposed to fetch by grabbing the current version of
                    // META. However, note that there is a case where we well
                    // and truly have to give up: when the compaction frontier
                    // has advanced past the ts this snapshot is reading at.
                    //
                    // TODO: Figure out a meaningful timestamp to use here? As
                    // mentioned above, this error will eventually represent
                    // something totally unrecoverable, so it should probably go
                    // to the system errors (once that's built) instead of this
                    // err_stream. Aljoscha suggests that system errors likely
                    // won't have a timestamp in the source domain
                    // (https://github.com/MaterializeInc/materialize/pull/8212#issuecomment-915877541),
                    // so perhaps this problem just goes away at some point. In
                    // the meantime, we never downgrade capabilities on the
                    // err_stream until the operator is finished, so it
                    // technically works to emit it at ts=0 for now.
                    (Err(err.to_string()), 0, 1)
                }
            }
        })
    }
}
