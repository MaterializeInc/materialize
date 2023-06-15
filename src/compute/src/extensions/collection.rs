// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Extensions to Differential collections.

use std::hash::{BuildHasher, Hash, Hasher};

use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::{Arranged, TraceAgent};
use differential_dataflow::trace::{Batch, Trace, TraceReader};
use differential_dataflow::{Collection, Data, ExchangeData};
use timely::container::columnation::Columnation;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::Scope;

use crate::extensions::arrange::{ArrangementSize, MzArrange};

/// Extension methods for differential [`Collection`]s.
pub(crate) trait ConsolidateExt<G, D1, R>
where
    G: Scope,
    G::Timestamp: Lattice + Data,
    D1: ExchangeData + Hash,
    R: Semigroup + ExchangeData,
{
    /// Consolidates the collection if `must_consolidate` is `true` and leaves it
    /// untouched otherwise.
    fn mz_consolidate_if<Tr>(&self, must_consolidate: bool, name: &str) -> Self
    where
        Tr: Trace + TraceReader<Key = D1, Val = (), Time = G::Timestamp, R = R> + 'static,
        Tr::Batch: Batch,
        Arranged<G, TraceAgent<Tr>>: ArrangementSize;

    /// Consolidates the collection.
    fn mz_consolidate<Tr>(&self, name: &str) -> Self
    where
        Tr: Trace + TraceReader<Key = D1, Val = (), Time = G::Timestamp, R = R> + 'static,
        Tr::Batch: Batch,
        Arranged<G, TraceAgent<Tr>>: ArrangementSize;
}

impl<G, D1, R> ConsolidateExt<G, D1, R> for Collection<G, D1, R>
where
    G: Scope,
    G::Timestamp: Lattice + Data,
    D1: ExchangeData + Hash + Columnation,
    R: Semigroup + ExchangeData + Columnation,
{
    fn mz_consolidate_if<Tr>(&self, must_consolidate: bool, name: &str) -> Self
    where
        Tr: Trace + TraceReader<Key = D1, Val = (), Time = G::Timestamp, R = R> + 'static,
        Tr::Batch: Batch,
        Arranged<G, TraceAgent<Tr>>: ArrangementSize,
    {
        if must_consolidate {
            self.mz_consolidate(name)
        } else {
            self.clone()
        }
    }

    fn mz_consolidate<Tr>(&self, name: &str) -> Self
    where
        Tr: Trace + TraceReader<Key = D1, Val = (), Time = G::Timestamp, R = R> + 'static,
        Tr::Batch: Batch,
        Arranged<G, TraceAgent<Tr>>: ArrangementSize,
    {
        // We employ AHash below instead of the default hasher in DD to obtain
        // a better distribution of data to workers. AHash claims empirically
        // both speed and high quality, according to
        // https://github.com/tkaitchuck/aHash/blob/master/compare/readme.md.
        // TODO(vmarcos): Consider here if it is worth it to spend the time to
        // implement twisted tabulation hashing as proposed in Mihai Patrascu,
        // Mikkel Thorup: Twisted Tabulation Hashing. SODA 2013: 209-228, available
        // at https://epubs.siam.org/doi/epdf/10.1137/1.9781611973105.16. The latter
        // would provide good bounds for balls-into-bins problems when the number of
        // bins is small (as is our case), so we'd have a theoretical guarantee.
        // NOTE: We fix the seeds of a RandomState instance explicity with the same
        // seeds that would be given by `AHash` via ahash::AHasher::default() so as
        // to avoid a different selection due to compile-time features being differently
        // selected in other dependencies using `AHash` vis-à-vis cargo's strategy
        // of unioning features. This implies that we end up employ the fallback
        // hasher of `AHash`, but it should be sufficient for our needs.
        let random_state = ahash::RandomState::with_seeds(
            0x243f_6a88_85a3_08d3,
            0x1319_8a2e_0370_7344,
            0xa409_3822_299f_31d0,
            0x082e_fa98_ec4e_6c89,
        );
        let exchange = Exchange::new(move |update: &((D1, _), G::Timestamp, R)| {
            let data = &(update.0).0;
            let mut h = random_state.build_hasher();
            data.hash(&mut h);
            h.finish()
        });
        self.map(|k| (k, ()))
            .mz_arrange_core::<_, Tr>(exchange, name)
            .as_collection(|d: &D1, _| d.clone())
    }
}
