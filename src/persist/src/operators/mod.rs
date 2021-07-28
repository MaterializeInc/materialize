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

use timely::dataflow::operators::generic::operator;
use timely::dataflow::operators::ToStream;
use timely::dataflow::{Scope, Stream};
use timely::Data as TimelyData;

use crate::indexed::runtime::StreamReadHandle;
use crate::indexed::Snapshot;
use crate::Codec;

pub mod input;
pub mod source;
pub mod stream;

fn replay<G: Scope<Timestamp = u64>, K: TimelyData + Codec, V: TimelyData + Codec>(
    scope: &mut G,
    read: &StreamReadHandle<K, V>,
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
        match read.snapshot() {
            Ok(mut snap) => {
                while snap.read(&mut buf) {
                    for update in buf.drain(..) {
                        match flatten_decoded_update(update) {
                            Ok(u) => ok.push(u),
                            Err(errs) => errors.extend(errs),
                        }
                    }
                }
            }
            Err(err) => {
                // TODO: Figure out how to make these retractable.
                let err_str = format!("replaying persisted data: {}", err);
                errors.push((err_str, 0u64, 1isize));
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
