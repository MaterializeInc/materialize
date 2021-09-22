// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A Timely Dataflow operator that passes through its input after persisting
//! it.

use persist_types::Codec;
use timely::dataflow::operators::generic::operator;
use timely::dataflow::operators::{Concat, OkErr, ToStream};
use timely::dataflow::{Scope, Stream};
use timely::Data as TimelyData;

use crate::indexed::runtime::{StreamReadHandle, StreamWriteHandle};
use crate::operators;

/// A Timely Dataflow operator that passes through its input after persisting
/// it.
pub trait Persist<G: Scope<Timestamp = u64>, K: TimelyData, V: TimelyData> {
    /// Persist each element of the stream, emitting it after it's been durably
    /// recorded.
    fn persist(
        &self,
        token: (StreamWriteHandle<K, V>, StreamReadHandle<K, V>),
    ) -> (
        Stream<G, ((K, V), u64, isize)>,
        Stream<G, (String, u64, isize)>,
    );
}

impl<G, K, V> Persist<G, K, V> for Stream<G, ((K, V), u64, isize)>
where
    G: Scope<Timestamp = u64>,
    K: TimelyData + Codec,
    V: TimelyData + Codec,
{
    fn persist(
        &self,
        token: (StreamWriteHandle<K, V>, StreamReadHandle<K, V>),
    ) -> (
        Stream<G, ((K, V), u64, isize)>,
        Stream<G, (String, u64, isize)>,
    ) {
        let (write, read) = token;

        let (ok_new, err_new) = self.ok_err(
            move |((k, v), ts, diff)| -> Result<((K, V), u64, isize), (String, u64, isize)> {
                let res = write.write(&[((k.clone(), v.clone()), ts, diff)]);
                // TODO: Don't do this synchronously.
                res.recv().map_err(|err| {
                    let err_str = format!("persisting data: {}", err);
                    (err_str, ts, 1)
                })?;
                Ok(((k, v), ts, diff))
            },
        );

        // Replay the previously persisted data, if any.
        let (ok_previous, err_previous) = match read.snapshot() {
            Err(err) => (
                operator::empty(&self.scope()),
                // TODO: Figure out how to make these retractable.
                vec![(format!("replaying persisted data: {}", err), 0, 1)]
                    .to_stream(&mut self.scope()),
            ),
            Ok(snapshot) => operators::replay(&mut self.scope(), snapshot),
        };

        let ok_all = ok_previous.concat(&ok_new);
        let err_all = err_previous.concat(&err_new);
        (ok_all, err_all)
    }
}

#[cfg(test)]
mod tests {
    use timely::dataflow::operators::capture::Extract;
    use timely::dataflow::operators::generic::operator;
    use timely::dataflow::operators::input::Handle;
    use timely::dataflow::operators::probe::Probe;
    use timely::dataflow::operators::Capture;

    use crate::error::Error;
    use crate::mem::MemRegistry;
    use crate::unreliable::UnreliableHandle;

    use super::*;

    #[test]
    fn persist() -> Result<(), Error> {
        let mut registry = MemRegistry::new();
        let p = registry.runtime_no_reentrance()?;

        timely::execute_directly(move |worker| {
            let (mut input, probe) = worker.dataflow(|scope| {
                let token = p.create_or_load("1").unwrap();
                let mut input = Handle::new();
                let (ok_stream, _) = input.to_stream(scope).persist(token);
                let probe = ok_stream.probe();
                (input, probe)
            });
            for i in 1..=5 {
                input.send(((i.to_string(), ()), i, 1));
            }
            input.advance_to(6);
            while probe.less_than(&6) {
                worker.step();
            }
        });

        // Execute a second dataflow and reuse the previous in-memory state.
        // This exists to simulate what would happen after a restart in a Persister
        // that was actually backed by persistent storage
        let p = registry.runtime_no_reentrance()?;
        let recv = timely::execute_directly(move |worker| {
            let (mut input, recv) = worker.dataflow(|scope| {
                let token = p.create_or_load("1").unwrap();
                let mut input = Handle::new();
                let (ok_stream, _) = input.to_stream(scope).persist(token);
                let recv = ok_stream.capture();
                (input, recv)
            });
            for i in 6..=9 {
                input.send(((i.to_string(), ()), i, 1));
            }
            recv
        });

        let mut actual = recv
            .extract()
            .into_iter()
            .flat_map(|(_, xs)| xs.into_iter().map(|((k, _), _, _)| k))
            .collect::<Vec<_>>();
        actual.sort();
        let expected = (1usize..=9usize).map(|x| x.to_string()).collect::<Vec<_>>();
        assert_eq!(actual, expected);

        Ok(())
    }

    #[test]
    fn error_stream() -> Result<(), Error> {
        let mut unreliable = UnreliableHandle::default();
        let p = MemRegistry::new().runtime_unreliable(unreliable.clone())?;
        let token = p.create_or_load::<(), ()>("error_stream").unwrap();
        unreliable.make_unavailable();

        let recv = timely::execute_directly(move |worker| {
            worker.dataflow(|scope| {
                let stream = operator::empty(scope);
                let (_, err_stream) = stream.persist(token);
                err_stream.capture()
            })
        });

        let actual = recv
            .extract()
            .into_iter()
            .flat_map(|(_, xs)| xs.into_iter())
            .collect::<Vec<_>>();

        let expected = vec![(
            "replaying persisted data: failed to commit metadata after appending to unsealed: unavailable: blob set".to_string(),
            0,
            1,
        )];
        assert_eq!(actual, expected);

        Ok(())
    }
}
