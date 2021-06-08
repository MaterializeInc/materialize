// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A Timely Dataflow operator that synchronously persists stream input.

use timely::dataflow::channels::pushers::buffer::AutoflushSession;
use timely::dataflow::channels::pushers::{Counter, Tee};
use timely::dataflow::operators::unordered_input::UnorderedHandle;
use timely::dataflow::operators::{ActivateCapability, Concat, ToStream, UnorderedInput};
use timely::dataflow::{Scope, Stream};
use timely::scheduling::ActivateOnDrop;

use crate::persister::{Meta, Snapshot, Write};
use crate::Token;

/// A persistent equivalent of [UnorderedInput].
pub trait PersistentUnorderedInput<G: Scope<Timestamp = u64>> {
    /// A persistent equivalent of [UnorderedInput::new_unordered_input].
    fn new_persistent_unordered_input<W: Write + 'static, M: Meta>(
        &mut self,
        token: Token<W, M>,
    ) -> (
        (PersistentUnorderedHandle, ActivateCapability<G::Timestamp>),
        Stream<G, (String, u64, isize)>,
    );
}

impl<G> PersistentUnorderedInput<G> for G
where
    G: Scope<Timestamp = u64>,
{
    fn new_persistent_unordered_input<W: Write + 'static, M: Meta>(
        &mut self,
        token: Token<W, M>,
    ) -> (
        (PersistentUnorderedHandle, ActivateCapability<G::Timestamp>),
        Stream<G, (String, u64, isize)>,
    ) {
        let ((handle, cap), stream) = self.new_unordered_input();
        let (write, meta) = token.into_inner();

        // Replay the previously persisted data, if any.
        //
        // TODO: Do this with a timely operator that reads the snapshot.
        let previously_persisted = {
            let mut snap = meta.snapshot().expect("TODO");
            let mut buf = Vec::new();
            while snap.read(&mut buf) {}
            buf.into_iter()
                .map(|((key, _), ts, diff)| (key, ts, diff))
                .to_stream(self)
        };

        let handle = PersistentUnorderedHandle {
            write: Box::new(write),
            handle,
        };
        ((handle, cap), previously_persisted.concat(&stream))
    }
}

/// A persistent equivalent of [UnorderedHandle].
pub struct PersistentUnorderedHandle {
    write: Box<dyn Write>,
    handle: UnorderedHandle<u64, (String, u64, isize)>,
}

impl PersistentUnorderedHandle {
    /// A persistent equivalent of [UnorderedHandle::session].
    pub fn session<'b>(
        &'b mut self,
        cap: ActivateCapability<u64>,
    ) -> PersistentUnorderedSession<'b> {
        PersistentUnorderedSession {
            write: &mut self.write,
            session: self.handle.session(cap),
        }
    }
}

/// A persistent equivalent of [UnorderedHandle::session]'s return type.
pub struct PersistentUnorderedSession<'b> {
    write: &'b mut Box<dyn Write>,
    session: ActivateOnDrop<
        AutoflushSession<
            'b,
            u64,
            (String, u64, isize),
            Counter<u64, (String, u64, isize), Tee<u64, (String, u64, isize)>>,
        >,
    >,
}

impl<'b> PersistentUnorderedSession<'b> {
    /// Transmits a single record after synchronously persisting it.
    pub fn give(&mut self, data: (String, u64, isize)) {
        self.write
            .write_sync(&[((data.0.clone(), String::new()), data.1, data.2)])
            .expect("TODO");
        self.session.give(data);
    }
}

#[cfg(test)]
mod tests {
    use timely::dataflow::operators::capture::Extract;
    use timely::dataflow::operators::Capture;

    use crate::error::Error;
    use crate::mem::MemPersister;
    use crate::persister::Persister;
    use crate::Id;

    use super::*;

    #[test]
    fn new_persistent_unordered_input() -> Result<(), Error> {
        let mut p = MemPersister::new();

        let dataz = timely::execute_directly(move |worker| {
            let (mut handle, cap) = worker.dataflow(|scope| {
                let persister = p.create_or_load(Id(1)).unwrap();
                let (input, _stream) = scope.new_persistent_unordered_input(persister);
                input
            });
            let mut session = handle.session(cap);
            for i in 1..=5 {
                session.give((i.to_string(), i, 1));
            }
            p.into_inner()
        });

        let mut p = MemPersister::from_inner(dataz);
        let recv = timely::execute_directly(move |worker| {
            let ((mut handle, cap), recv) = worker.dataflow(|scope| {
                let persister = p.create_or_load(Id(1)).unwrap();
                let (input, stream) = scope.new_persistent_unordered_input(persister);
                let recv = stream.capture();
                (input, recv)
            });
            let mut session = handle.session(cap);
            for i in 6..=9 {
                session.give((i.to_string(), i, 1));
            }
            recv
        });

        let mut actual = recv
            .extract()
            .into_iter()
            .flat_map(|(_, xs)| xs.into_iter().map(|x| x.0))
            .collect::<Vec<_>>();
        actual.sort();
        let expected = (1usize..=9usize).map(|x| x.to_string()).collect::<Vec<_>>();
        assert_eq!(actual, expected);

        Ok(())
    }
}
