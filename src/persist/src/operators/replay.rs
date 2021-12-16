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
    /// Emits each record in a snapshot, yielding periodically.
    ///
    /// This yields after `outputs_per_yield` outputs to allow downstream
    /// operators to reduce down the data and limit max memory usage.
    fn replay(
        &self,
        snapshot: Result<DecodedSnapshot<K, V>, Error>,
        outputs_per_yield: usize,
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
        outputs_per_yield: usize,
    ) -> Stream<G, (Result<(K, V), String>, u64, isize)> {
        // TODO: This currently works by only emitting the persisted
        // data on worker 0 because that was the simplest thing to do
        // initially. Instead, we should shard up the responsibility
        // between all the workers.
        let active_worker = self.index() == 0;

        let result_stream: Stream<G, Result<((K, V), u64, isize), Error>> =
            operator::source(self, "Replay", move |cap, info| {
                let activator = self.activator_for(&info.address[..]);
                let mut snapshot_cap = if active_worker {
                    Some((snapshot.map(|s| (s.since(), s.into_iter())), cap))
                } else {
                    None
                };

                move |output| {
                    let mut done = true;
                    let (snapshot, cap) = match snapshot_cap.as_mut() {
                        Some(x) => x,
                        None => return, // We already consumed our snapshot.
                    };

                    let mut session = output.session(&cap);

                    match snapshot {
                        Ok((snapshot_since, snapshot_iter)) => {
                            // NB: This `idx` from enumerate resets back to 0
                            // each time the operator is run.
                            for (idx, x) in snapshot_iter.enumerate() {
                                if let Ok((_, ts, _)) = &x {
                                    // The raw update data held internally in the
                                    // snapshot may not be physically compacted up to
                                    // the logical compaction frontier of since.
                                    // Snapshot handles advancing any necessary data but
                                    // we double check that invariant here.
                                    debug_assert!(snapshot_since.less_equal(ts));
                                }
                                session.give(x);
                                if idx + 1 >= outputs_per_yield {
                                    done = false;
                                    break;
                                }
                            }
                        }
                        Err(e) => {
                            session.give(Err(Error::String(format!(
                                "replaying persisted data: {}",
                                e
                            ))));
                        }
                    }

                    if done {
                        snapshot_cap.take();
                    } else {
                        activator.activate();
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

#[cfg(test)]
mod tests {
    use timely::dataflow::channels::pact::Pipeline;
    use timely::dataflow::operators::capture::Extract;
    use timely::dataflow::operators::{Capture, OkErr, Operator};

    use crate::error::Error;
    use crate::indexed::ListenFn;
    use crate::mem::MemRegistry;
    use crate::operators::split_ok_err;

    use super::*;

    #[test]
    fn replay() -> Result<(), Error> {
        let mut registry = MemRegistry::new();
        let p = registry.runtime_no_reentrance()?;

        let (write, _) = p.create_or_load("1");
        for i in 1..=100 {
            write
                .write(&[((format!("{:03}", i), ()), i, 1)])
                .recv()
                .expect("write was successful");
        }
        write.seal(101).recv().expect("seal was successful");

        let (oks, errs) = timely::execute_directly(move |worker| {
            let (oks, errs) = worker.dataflow(|scope| {
                let (_, read) = p.create_or_load::<String, ()>("1");
                let snapshot = read.listen(ListenFn(Box::new(|_| {})));
                let (ok_stream, err_stream) = scope.replay(snapshot, 3).ok_err(split_ok_err);
                // Preserve the batching structure out of replay so we can
                // assert on it.
                let ok_stream = ok_stream.unary(Pipeline, "Batches", move |_, _| {
                    move |input, output| {
                        input.for_each(|time, data| {
                            let mut batch = Vec::new();
                            data.swap(&mut batch);
                            output.session(&time).give(batch);
                        });
                    }
                });
                (ok_stream.capture(), err_stream.capture())
            });

            (oks, errs)
        });

        assert_eq!(
            errs.extract()
                .into_iter()
                .flat_map(|(_time, data)| data.into_iter().map(|(err, _ts, _diff)| err))
                .collect::<Vec<_>>(),
            Vec::<String>::new()
        );

        let actual = oks
            .extract()
            .into_iter()
            .flat_map(|(_, batches)| batches)
            .collect::<Vec<_>>();

        let expected = (1u64..=100u64)
            .map(|x| ((format!("{:03}", x), ()), x, 1))
            .collect::<Vec<_>>()
            .chunks(3)
            .map(|batch| batch.to_vec())
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);

        Ok(())
    }
}
