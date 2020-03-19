// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use differential_dataflow::difference::Semigroup;
use differential_dataflow::AsCollection;
use differential_dataflow::Collection;
use timely::dataflow::channels::pact::{ParallelizationContract, Pipeline};
use timely::dataflow::channels::pushers::Tee;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::generic::{InputHandle, OperatorInfo, OutputHandle};
use timely::dataflow::operators::Capability;
use timely::dataflow::{Scope, Stream};
use timely::Data;

pub trait CollectionExt<G, D1, R>
where
    G: Scope,
    R: Semigroup,
{
    fn map_fallible<D2, E, L>(&self, logic: L) -> (Collection<G, D2, R>, Collection<G, E, R>)
    where
        D2: Data,
        E: Data,
        L: FnMut(D1) -> Result<D2, E> + 'static;

    fn flat_map_fallible<D2, E, I, L>(
        &self,
        logic: L,
    ) -> (Collection<G, D2, R>, Collection<G, E, R>)
    where
        D2: Data,
        E: Data,
        I: IntoIterator<Item = Result<D2, E>>,
        L: FnMut(D1) -> I + 'static;

    fn filter_fallible<E, L>(&self, logic: L) -> (Collection<G, D1, R>, Collection<G, E, R>)
    where
        E: Data,
        L: FnMut(&D1) -> Result<bool, E> + 'static;
}

impl<G, D1, R> CollectionExt<G, D1, R> for Collection<G, D1, R>
where
    G: Scope,
    G::Timestamp: Data,
    D1: Data,
    R: Semigroup,
{
    fn map_fallible<D2, E, L>(&self, mut logic: L) -> (Collection<G, D2, R>, Collection<G, E, R>)
    where
        D2: Data,
        E: Data,
        L: FnMut(D1) -> Result<D2, E> + 'static,
    {
        let (ok_stream, err_stream) = self.inner.map_fallible(move |(d1, t, r)| match logic(d1) {
            Ok(d2) => Ok((d2, t, r)),
            Err(e) => Err((e, t, r)),
        });
        (ok_stream.as_collection(), err_stream.as_collection())
    }

    fn flat_map_fallible<D2, E, I, L>(
        &self,
        mut logic: L,
    ) -> (Collection<G, D2, R>, Collection<G, E, R>)
    where
        D2: Data,
        E: Data,
        I: IntoIterator<Item = Result<D2, E>>,
        L: FnMut(D1) -> I + 'static,
    {
        let (ok_stream, err_stream) = self.inner.flat_map_fallible(move |(d1, t, r)| {
            logic(d1).into_iter().map(move |res| match res {
                Ok(d2) => Ok((d2, t.clone(), r.clone())),
                Err(e) => Err((e, t.clone(), r.clone())),
            })
        });
        (ok_stream.as_collection(), err_stream.as_collection())
    }

    fn filter_fallible<E, L>(&self, mut logic: L) -> (Collection<G, D1, R>, Collection<G, E, R>)
    where
        E: Data,
        L: FnMut(&D1) -> Result<bool, E> + 'static,
    {
        let (ok_stream, err_stream) =
            self.inner
                .filter_fallible(move |(d1, t, r)| match logic(d1) {
                    Ok(retain) => Ok(retain),
                    Err(e) => Err((e, t.clone(), r.clone())),
                });
        (ok_stream.as_collection(), err_stream.as_collection())
    }
}

pub trait StreamExt<G, D1>
where
    D1: Data,
    G: Scope,
{
    fn unary_fallible<D2, E, B, L, P>(
        &self,
        pact: P,
        name: &str,
        constructor: B,
    ) -> (Stream<G, D2>, Stream<G, E>)
    where
        D2: Data,
        E: Data,
        B: FnOnce(Capability<G::Timestamp>, OperatorInfo) -> L,
        L: FnMut(
                &mut InputHandle<G::Timestamp, D1, P::Puller>,
                &mut OutputHandle<G::Timestamp, D2, Tee<G::Timestamp, D2>>,
                &mut OutputHandle<G::Timestamp, E, Tee<G::Timestamp, E>>,
            ) + 'static,
        P: ParallelizationContract<G::Timestamp, D1>;

    fn map_fallible<D2, E, L>(&self, logic: L) -> (Stream<G, D2>, Stream<G, E>)
    where
        D2: Data,
        E: Data,
        L: FnMut(D1) -> Result<D2, E> + 'static;

    fn flat_map_fallible<D2, E, I, L>(&self, logic: L) -> (Stream<G, D2>, Stream<G, E>)
    where
        D2: Data,
        E: Data,
        I: IntoIterator<Item = Result<D2, E>>,
        L: FnMut(D1) -> I + 'static;

    fn filter_fallible<E, L>(&self, logic: L) -> (Stream<G, D1>, Stream<G, E>)
    where
        E: Data,
        L: FnMut(&D1) -> Result<bool, E> + 'static;
}

impl<G, D1> StreamExt<G, D1> for Stream<G, D1>
where
    D1: Data,
    G: Scope,
{
    fn unary_fallible<D2, E, B, L, P>(
        &self,
        pact: P,
        name: &str,
        constructor: B,
    ) -> (Stream<G, D2>, Stream<G, E>)
    where
        D2: Data,
        E: Data,
        B: FnOnce(Capability<G::Timestamp>, OperatorInfo) -> L,
        L: FnMut(
                &mut InputHandle<G::Timestamp, D1, P::Puller>,
                &mut OutputHandle<G::Timestamp, D2, Tee<G::Timestamp, D2>>,
                &mut OutputHandle<G::Timestamp, E, Tee<G::Timestamp, E>>,
            ) + 'static,
        P: ParallelizationContract<G::Timestamp, D1>,
    {
        let mut builder = OperatorBuilder::new(name.into(), self.scope());
        builder.set_notify(false);

        let operator_info = builder.operator_info();

        let mut input = builder.new_input(self, pact);
        let (mut ok_output, ok_stream) = builder.new_output();
        let (mut err_output, err_stream) = builder.new_output();

        builder.build(move |mut capabilities| {
            // `capabilities` should be a single-element vector.
            let capability = capabilities.pop().unwrap();
            let mut logic = constructor(capability, operator_info);
            move |_frontiers| {
                let mut ok_output_handle = ok_output.activate();
                let mut err_output_handle = err_output.activate();
                logic(&mut input, &mut ok_output_handle, &mut err_output_handle);
            }
        });

        (ok_stream, err_stream)
    }

    fn map_fallible<D2, E, L>(&self, mut logic: L) -> (Stream<G, D2>, Stream<G, E>)
    where
        D2: Data,
        E: Data,
        L: FnMut(D1) -> Result<D2, E> + 'static,
    {
        let mut storage = Vec::new();
        self.unary_fallible(Pipeline, "MapFallible", move |_, _| {
            move |input, ok_output, err_output| {
                input.for_each(|time, data| {
                    let mut ok_session = ok_output.session(&time);
                    let mut err_session = err_output.session(&time);
                    data.swap(&mut storage);
                    for d1 in storage.drain(..) {
                        match logic(d1) {
                            Ok(d2) => ok_session.give(d2),
                            Err(e) => err_session.give(e),
                        }
                    }
                })
            }
        })
    }

    fn flat_map_fallible<D2, E, I, L>(&self, mut logic: L) -> (Stream<G, D2>, Stream<G, E>)
    where
        D2: Data,
        E: Data,
        I: IntoIterator<Item = Result<D2, E>>,
        L: FnMut(D1) -> I + 'static,
    {
        let mut storage = Vec::new();
        self.unary_fallible(Pipeline, "FlatMapFallible", move |_, _| {
            move |input, ok_output, err_output| {
                input.for_each(|time, data| {
                    let mut ok_session = ok_output.session(&time);
                    let mut err_session = err_output.session(&time);
                    data.swap(&mut storage);
                    for r in storage.drain(..).flat_map(|d1| logic(d1)) {
                        match r {
                            Ok(d2) => ok_session.give(d2),
                            Err(e) => err_session.give(e),
                        }
                    }
                })
            }
        })
    }

    fn filter_fallible<E, L>(&self, mut logic: L) -> (Stream<G, D1>, Stream<G, E>)
    where
        E: Data,
        L: FnMut(&D1) -> Result<bool, E> + 'static,
    {
        let mut storage = Vec::new();
        self.unary_fallible(Pipeline, "FilterFallible", move |_, _| {
            move |input, ok_output, err_output| {
                input.for_each(|time, data| {
                    let mut ok_session = ok_output.session(&time);
                    let mut err_session = err_output.session(&time);
                    data.swap(&mut storage);
                    storage.retain(|d1| match logic(d1) {
                        Ok(retain) => retain,
                        Err(e) => {
                            err_session.give(e);
                            false
                        }
                    });
                    // TODO(benesch): shouldn't Timely do this check internally?
                    if !storage.is_empty() {
                        ok_session.give_vec(&mut storage);
                    }
                })
            }
        })
    }
}
