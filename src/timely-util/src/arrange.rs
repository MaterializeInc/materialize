// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::{Arrange, Arranged, TraceAgent};
use differential_dataflow::trace::{Batch, Trace, TraceReader};
use differential_dataflow::{Collection, Data, ExchangeData, Hashable};
use timely::container::columnation::Columnation;
use timely::dataflow::channels::pact::ParallelizationContract;
use timely::dataflow::{Scope, ScopeParent};

pub trait MzArrange
where
    <Self::Scope as ScopeParent>::Timestamp: Lattice,
{
    type Scope: Scope;
    type Key: Data;
    type Val: Data;
    type R: Data + Semigroup;

    /// Arranges a stream of `(Key, Val)` updates by `Key`. Accepts an empty instance of the trace type.
    ///
    /// This operator arranges a stream of values into a shared trace, whose contents it maintains.
    /// This trace is current for all times marked completed in the output stream, and probing this stream
    /// is the correct way to determine that times in the shared trace are committed.
    fn mz_arrange<Tr>(&self, name: &str) -> Arranged<Self::Scope, TraceAgent<Tr>>
    where
        Self::Key: ExchangeData + Hashable,
        Self::Val: ExchangeData,
        Self::R: ExchangeData,
        Tr: Trace
            + TraceReader<
                Key = Self::Key,
                Val = Self::Val,
                Time = <Self::Scope as ScopeParent>::Timestamp,
                R = Self::R,
            > + 'static,
        Tr::Batch: Batch;

    /// Arranges a stream of `(Key, Val)` updates by `Key`. Accepts an empty instance of the trace type.
    ///
    /// This operator arranges a stream of values into a shared trace, whose contents it maintains.
    /// This trace is current for all times marked completed in the output stream, and probing this stream
    /// is the correct way to determine that times in the shared trace are committed.
    fn mz_arrange_core<P, Tr>(&self, pact: P, name: &str) -> Arranged<Self::Scope, TraceAgent<Tr>>
    where
        P: ParallelizationContract<
            <Self::Scope as ScopeParent>::Timestamp,
            (
                (Self::Key, Self::Val),
                <Self::Scope as ScopeParent>::Timestamp,
                Self::R,
            ),
        >,
        Tr: Trace
            + TraceReader<
                Key = Self::Key,
                Val = Self::Val,
                Time = <Self::Scope as ScopeParent>::Timestamp,
                R = Self::R,
            > + 'static,
        Tr::Batch: Batch;
}

impl<G, K, V, R> MzArrange for Collection<G, (K, V), R>
where
    G: Scope,
    G::Timestamp: Lattice,
    K: Data + Columnation,
    V: Data + Columnation,
    R: Semigroup + Columnation,
{
    type Scope = G;
    type Key = K;
    type Val = V;
    type R = R;

    fn mz_arrange<Tr>(&self, name: &str) -> Arranged<G, TraceAgent<Tr>>
    where
        K: ExchangeData + Hashable,
        V: ExchangeData,
        R: ExchangeData,
        Tr: Trace + TraceReader<Key = K, Val = V, Time = G::Timestamp, R = R> + 'static,
        Tr::Batch: Batch,
    {
        // Allow access to `arrange_named` because we're within Mz's wrapper.
        #[allow(clippy::disallowed_methods)]
        self.arrange_named(name)
    }

    fn mz_arrange_core<P, Tr>(&self, pact: P, name: &str) -> Arranged<G, TraceAgent<Tr>>
    where
        P: ParallelizationContract<G::Timestamp, ((K, V), G::Timestamp, R)>,
        Tr: Trace + TraceReader<Key = K, Val = V, Time = G::Timestamp, R = R> + 'static,
        Tr::Batch: Batch,
    {
        // Allow access to `arrange_named` because we're within Mz's wrapper.
        #[allow(clippy::disallowed_methods)]
        self.arrange_core(pact, name)
    }
}

pub struct KeyCollection<G: Scope, D, R: Semigroup = usize>(Collection<G, D, R>);

pub trait IntoKeyCollection {
    type Output;
    fn into_key_collection(self) -> Self::Output;
}

impl<G: Scope, D, R: Semigroup> IntoKeyCollection for Collection<G, D, R> {
    type Output = KeyCollection<G, D, R>;

    fn into_key_collection(self) -> Self::Output {
        KeyCollection(self)
    }
}

impl<G, K, R> MzArrange for KeyCollection<G, K, R>
where
    G: Scope,
    K: Data + Columnation,
    G::Timestamp: Lattice,
    R: Semigroup + Columnation,
{
    type Scope = G;
    type Key = K;
    type Val = ();
    type R = R;

    fn mz_arrange<Tr>(&self, name: &str) -> Arranged<G, TraceAgent<Tr>>
    where
        K: ExchangeData + Hashable,
        R: ExchangeData,
        Tr: Trace + TraceReader<Key = K, Val = (), Time = G::Timestamp, R = R> + 'static,
        Tr::Batch: Batch,
    {
        self.0.map(|d| (d, ())).mz_arrange(name)
    }

    fn mz_arrange_core<P, Tr>(&self, pact: P, name: &str) -> Arranged<G, TraceAgent<Tr>>
    where
        P: ParallelizationContract<G::Timestamp, ((K, ()), G::Timestamp, R)>,
        Tr: Trace + TraceReader<Key = K, Val = (), Time = G::Timestamp, R = R> + 'static,
        Tr::Batch: Batch,
    {
        self.0.map(|d| (d, ())).mz_arrange_core(pact, name)
    }
}
