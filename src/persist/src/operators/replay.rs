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
use timely::progress::Antichain;
use timely::{Data as TimelyData, PartialOrder};

use crate::error::Error;
use crate::indexed::runtime::DecodedSnapshot;
use crate::indexed::Snapshot;

/// Extension trait for [`Stream`].
pub trait Replay<G: Scope<Timestamp = u64>, K: TimelyData, V: TimelyData> {
    /// Emits each record in a snapshot.
    fn replay(
        &self,
        snapshot: Result<DecodedSnapshot<K, V>, Error>,
        as_of_frontier: &Antichain<u64>,
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
        as_of_frontier: &Antichain<u64>,
    ) -> Stream<G, (Result<(K, V), String>, u64, isize)> {
        // TODO: This currently works by only emitting the persisted
        // data on worker 0 because that was the simplest thing to do
        // initially. Instead, we should shard up the responsibility
        // between all the workers.
        let active_worker = self.index() == 0;

        let as_of_frontier = as_of_frontier.clone();

        let result_stream: Stream<G, Result<((K, V), u64, isize), Error>> = operator::source(
            self,
            "Replay",
            move |cap, _info| {
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

                            if PartialOrder::less_than(&as_of_frontier, &snapshot_since) {
                                session.give(Err(Error::String(format!(
                                    "replaying persisted data: snapshot since ({:?}) is beyond expected as_of ({:?})",
                                    snapshot_since, as_of_frontier
                                ))));

                                return;
                            }

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
            },
        );
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
    use timely::dataflow::operators::capture::Extract;
    use timely::dataflow::operators::{Capture, OkErr};

    use crate::error::Error;
    use crate::mem::MemRegistry;
    use crate::operators::split_ok_err;

    use super::*;

    #[test]
    fn replay() -> Result<(), Error> {
        let mut registry = MemRegistry::new();
        let p = registry.runtime_no_reentrance()?;

        let (write, _) = p.create_or_load("1");
        for i in 1..=5 {
            write
                .write(&[((i.to_string(), ()), i, 1)])
                .recv()
                .expect("write was successful");
        }
        write.seal(6).recv().expect("seal was successful");

        let (oks, errs) = timely::execute_directly(move |worker| {
            let (oks, errs) = worker.dataflow(|scope| {
                let (_write, read) = p.create_or_load::<String, ()>("1");
                let (ok_stream, err_stream) = scope
                    .replay(read.snapshot(), &Antichain::from_elem(0))
                    .ok_err(split_ok_err);
                (ok_stream.capture(), err_stream.capture())
            });

            (oks, errs)
        });

        assert!(errs
            .extract()
            .into_iter()
            .flat_map(|(_time, data)| data.into_iter().map(|(err, _ts, _diff)| err))
            .collect::<Vec<_>>()
            .is_empty());

        let mut actual = oks
            .extract()
            .into_iter()
            .flat_map(|(_, xs)| xs.into_iter().map(|((k, _), _, _)| k))
            .collect::<Vec<_>>();
        actual.sort();

        let expected = (1usize..=5usize).map(|x| x.to_string()).collect::<Vec<_>>();
        assert_eq!(actual, expected);

        Ok(())
    }

    #[test]
    fn replay_invalid_as_of() -> Result<(), Error> {
        let mut registry = MemRegistry::new();
        let p = registry.runtime_no_reentrance()?;

        let (write, _) = p.create_or_load("1");
        for i in 1..=5 {
            write
                .write(&[((i.to_string(), ()), i, 1)])
                .recv()
                .expect("write was successful");
        }
        write.seal(6).recv().expect("seal was successful");
        write
            .allow_compaction(Antichain::from_elem(5))
            .recv()
            .expect("compaction was successful");

        let (oks, errs) = timely::execute_directly(move |worker| {
            let (oks, errs) = worker.dataflow(|scope| {
                let (_write, read) = p.create_or_load::<String, ()>("1");
                let (ok_stream, err_stream) = scope
                    .replay(read.snapshot(), &Antichain::from_elem(0))
                    .ok_err(split_ok_err);
                (ok_stream.capture(), err_stream.capture())
            });

            (oks, errs)
        });

        assert!(oks
            .extract()
            .into_iter()
            .flat_map(|(_time, data)| data.into_iter().map(|(data, _ts, _diff)| data))
            .collect::<Vec<_>>()
            .is_empty());

        let errs = errs
            .extract()
            .into_iter()
            .flat_map(|(_time, data)| data.into_iter().map(|(err, _ts, _diff)| err))
            .collect::<Vec<_>>();

        assert_eq!(
            errs,
            vec!["replaying persisted data: snapshot since (Antichain { elements: [5] }) is beyond expected as_of (Antichain { elements: [0] })".to_string()]);

        Ok(())
    }
}
