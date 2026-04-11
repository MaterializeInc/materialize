// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use differential_dataflow::Data;
use differential_dataflow::difference::Abelian;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::{Arranged, TraceAgent};
use differential_dataflow::trace::implementations::merge_batcher::container::InternalMerge;
use differential_dataflow::trace::{Builder, Trace, TraceReader};
use timely::Container;
use timely::container::PushInto;
use timely::dataflow::Scope;

use crate::extensions::arrange::ArrangementSize;

/// Extension trait for the `reduce_abelian` differential dataflow method.
pub(crate) trait MzReduce<G: Scope, T1: TraceReader<Time = G::Timestamp>>
where
    G::Timestamp: Lattice,
{
    /// Applies `reduce` to arranged data, and returns an arrangement of output data.
    fn mz_reduce_abelian<L, Bu, T2>(self, name: &str, logic: L) -> Arranged<G, TraceAgent<T2>>
    where
        T2: for<'a> Trace<
                Key<'a> = T1::Key<'a>,
                KeyOwn = T1::KeyOwn,
                ValOwn: Data,
                Time = G::Timestamp,
                Diff: Abelian,
            > + 'static,
        Bu: Builder<Time = G::Timestamp, Output = T2::Batch>,
        Bu::Input:
            Container + InternalMerge + PushInto<((T1::KeyOwn, T2::ValOwn), T2::Time, T2::Diff)>,
        L: FnMut(T1::Key<'_>, &[(T1::Val<'_>, T1::Diff)], &mut Vec<(T2::ValOwn, T2::Diff)>)
            + 'static,
        Arranged<G, TraceAgent<T2>>: ArrangementSize;
}

impl<G, T1> MzReduce<G, T1> for Arranged<G, T1>
where
    G: Scope,
    G::Timestamp: Lattice,
    T1: TraceReader<Time = G::Timestamp, KeyOwn: Ord> + Clone + 'static,
{
    /// Applies `reduce` to arranged data, and returns an arrangement of output data.
    fn mz_reduce_abelian<L, Bu, T2>(self, name: &str, logic: L) -> Arranged<G, TraceAgent<T2>>
    where
        T2: for<'a> Trace<
                Key<'a> = T1::Key<'a>,
                KeyOwn = T1::KeyOwn,
                ValOwn: Data,
                Time = G::Timestamp,
                Diff: Abelian,
            > + 'static,
        Bu: Builder<
                Time = G::Timestamp,
                Input: Container
                           + InternalMerge
                           + PushInto<((T1::KeyOwn, T2::ValOwn), T2::Time, T2::Diff)>,
                Output = T2::Batch,
            >,
        L: FnMut(T1::Key<'_>, &[(T1::Val<'_>, T1::Diff)], &mut Vec<(T2::ValOwn, T2::Diff)>)
            + 'static,
        Arranged<G, TraceAgent<T2>>: ArrangementSize,
    {
        // Allow access to `reduce_abelian` since we're within Mz's wrapper and force arrangement size logging.
        #[allow(clippy::disallowed_methods)]
        Arranged::<_, _>::reduce_abelian::<_, Bu, T2>(self, name, logic).log_arrangement_size()
    }
}
