// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Timely and Differential Dataflow operators for persisting and replaying
//! data.

use persist_types::Codec;
use timely::dataflow::operators::generic::operator;
use timely::dataflow::operators::ToStream;
use timely::dataflow::{Scope, Stream};
use timely::Data as TimelyData;

use crate::indexed::runtime::DecodedSnapshot;
use crate::indexed::Snapshot;

pub mod input;
pub mod source;
pub mod stream;

fn replay<G: Scope<Timestamp = u64>, K: TimelyData + Codec, V: TimelyData + Codec>(
    scope: &mut G,
    mut snapshot: DecodedSnapshot<K, V>,
) -> (
    Stream<G, ((K, V), u64, isize)>,
    Stream<G, (String, u64, isize)>,
) {
    // TODO: This currently works by only emitting the persisted data on worker
    // 0 because that was the simplest thing to do initially. Instead, we should
    // shard up the responsibility between all the workers.
    if scope.index() == 0 {
        // TODO: Do this with a timely operator that reads the snapshot.
        let (mut buf, mut ok, mut errors) = (Vec::new(), Vec::new(), Vec::new());
        loop {
            let ret = match snapshot.read(&mut buf) {
                Ok(ret) => ret,
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
                    errors.push((err.to_string(), 0, 1));
                    continue;
                }
            };
            for update in buf.drain(..) {
                match flatten_decoded_update(update) {
                    Ok(u) => {
                        // The raw update data held internally in the snapshot
                        // may not be physically compacted up to the logical
                        // compaction frontier of since. Snapshot handles
                        // advancing any necessary data but we double check that
                        // invariant here.
                        debug_assert!(snapshot.since().less_equal(&u.1));
                        ok.push(u)
                    }
                    Err(errs) => errors.extend(errs),
                }
            }

            if ret == false {
                break;
            }
        }
        let ok_previous = ok.into_iter().to_stream(scope);
        let err_previous = errors.into_iter().to_stream(scope);
        (ok_previous, err_previous)
    } else {
        (operator::empty(scope), operator::empty(scope))
    }
}

fn flatten_decoded_update<K, V>(
    update: ((Result<K, String>, Result<V, String>), u64, isize),
) -> Result<((K, V), u64, isize), Vec<(String, u64, isize)>> {
    let ((k, v), ts, diff) = update;
    match (k, v) {
        (Ok(k), Ok(v)) => Ok(((k, v), ts, diff)),
        (Err(k_err), Ok(_)) => Err(vec![(k_err, ts, diff)]),
        (Ok(_), Err(v_err)) => Err(vec![(v_err, ts, diff)]),
        (Err(k_err), Err(v_err)) => Err(vec![(k_err, ts, diff), (v_err, ts, diff)]),
    }
}
