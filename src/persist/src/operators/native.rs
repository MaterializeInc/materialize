// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// WIP
#![allow(missing_docs, dead_code, unused_variables)]

//! An attempt at making durability that's as "native" as possible to
//! differential dataflow.

use std::future::Future;

use differential_dataflow::trace;
use persist_types::Codec;
use timely::dataflow::operators::input::Handle;
use timely::dataflow::operators::Input;
use timely::dataflow::{Scope, Stream};
use timely::progress::frontier::AntichainRef;
use timely::progress::Antichain;

// TODO:
// - compaction
// - mfp
// - a version with no keys (à la arrange_by_self)
// - non hardcoded timestamps and diffs
// - allow the user to pick their own OCC story

pub type Timestamp = u64;
pub type Diff = isize;

// WIP should this just be Description instead, even though lower is always
// exactly `Antichain::from_elem(Timestamp::minimum())`?
#[derive(Debug)]
pub struct DurabilityDesc {
    pub since: Antichain<Timestamp>,
    pub upper: Antichain<Timestamp>,
}

pub trait Durability: Sized {
    type Key: Codec;
    type Val: Codec;
    type Batch: trace::Batch<Self::Key, Self::Val, Timestamp, Diff> + Clone + 'static;
    // NB: I originally tried making `save_batch` an async fn, but rust then
    // seemed to insist that Self::Batch be Sync, which I don't think we want to
    // require
    type SaveF: Future<Output = Result<(), String>>;

    // WIP it's a bit odd to return DurabilityDesc here but it's only used at
    // dataflow construction time, so maybe it's fine? the OCC story plays in
    // here too possibly
    fn open() -> Result<(Self, DurabilityDesc), String>;

    // NB: retries internally. transient errors are retried, only returns error
    // when it's truly given up. only reasonable thing to do in response to an
    // error is panic
    fn save_batch(&self, batch: &Self::Batch) -> Self::SaveF;

    fn listen<G>(
        &self,
        as_of: AntichainRef<'_, Timestamp>,
        listener: Handle<Timestamp, Self::Batch>,
    );
}

trait DurableSink<K, V, B: trace::Batch<K, V, u64, isize>> {
    fn durable_sink<D>(&self, durability_system: &D)
    where
        D: Durability<Key = K, Val = V, Batch = B>;
}

trait DurableSource<G: Scope<Timestamp = Timestamp>> {
    fn durable_source<K, V, B, D>(
        &mut self,
        as_of: AntichainRef<'_, Timestamp>,
        durability_system: &D,
    ) -> Stream<G, B>
    where
        B: Clone + 'static,
        D: Durability<Key = K, Val = V, Batch = B>;
}

impl<G, K, V, B> DurableSink<K, V, B> for Stream<G, B>
where
    G: Scope<Timestamp = Timestamp>,
    B: trace::Batch<K, V, u64, isize> + Clone + 'static,
{
    fn durable_sink<D>(&self, _durability_system: &D)
    where
        D: Durability<Key = K, Val = V, Batch = B>,
    {
        todo!()
        // things to do here:
        // - maintain a buffer of batches waiting to be sent to save_batch
        //   - maybe consolidate these while they're hanging out
        // - maintain a vec of futures that haven't resolved yet
        // - use the activator+waker trick to get scheduled when futures resolve
    }
}

impl<G> DurableSource<G> for G
where
    G: Scope<Timestamp = Timestamp>,
{
    fn durable_source<K, V, B, D>(
        &mut self,
        as_of: AntichainRef<'_, Timestamp>,
        durability_system: &D,
    ) -> Stream<G, B>
    where
        B: Clone + 'static,
        D: Durability<Key = K, Val = V, Batch = B>,
    {
        let mut input = Handle::new();
        let stream = self.input_from(&mut input);
        durability_system.listen::<Self>(as_of, input);
        stream
    }
}

#[cfg(test)]
mod tests {
    use differential_dataflow::input::Input;
    use differential_dataflow::operators::arrange::ArrangeByKey;
    use differential_dataflow::trace::implementations::spine_fueled::Spine;
    use timely::dataflow::operators::{Inspect, Probe};

    use super::*;

    struct MyFuture;

    impl Future for MyFuture {
        type Output = Result<(), String>;

        fn poll(
            self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Self::Output> {
            todo!()
        }
    }

    // An impl of Durability for testing (atm just ensuring that things
    // type check).
    struct MyDurability<K, V, B>
    where
        B: trace::Batch<K, V, u64, isize>,
    {
        tr: Spine<K, V, Timestamp, Diff, B>,
    }

    impl<K, V, B> Durability for MyDurability<K, V, B>
    where
        K: Codec,
        V: Codec,
        B: trace::Batch<K, V, Timestamp, Diff> + Clone + 'static,
    {
        type Key = K;
        type Val = V;
        type Batch = B;
        type SaveF = MyFuture;

        fn open() -> Result<(Self, DurabilityDesc), String> {
            todo!()
        }

        fn save_batch(&self, batch: &Self::Batch) -> Self::SaveF {
            todo!()
        }

        fn listen<G>(
            &self,
            as_of: AntichainRef<'_, Timestamp>,
            listener: Handle<Timestamp, Self::Batch>,
        ) {
            todo!()
        }
    }

    #[test]
    fn vaguely_compiles() {
        const NUM_WORKERS: usize = 4;

        timely::execute(timely::Config::process(NUM_WORKERS), |worker| {
            let (durability_system, desc) = MyDurability::<String, String, _>::open().expect("WIP");

            let mut input = worker.dataflow(|scope| {
                let (input, coll) = scope.new_collection();
                coll.arrange_by_key()
                    .stream
                    .durable_sink(&durability_system);
                input
            });

            let probe = worker.dataflow(|scope| {
                scope
                    .durable_source(desc.since.borrow(), &durability_system)
                    .inspect(|x| eprintln!("batch: {:?}", x))
                    .probe()
            });

            for i in 1..10 {
                input.insert((i.to_string(), format!("v{}", i)));
                input.advance_to(i + 1);
                worker.step_while(|| probe.less_than(&input.time()));
            }
        })
        .expect("dataflow succeeds");
    }
}
