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

use differential_dataflow::difference::{Abelian, Semigroup};
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::{Arranged, TraceAgent};
use differential_dataflow::trace::cursor::IntoOwned;
use differential_dataflow::trace::{Batch, Builder, Trace, TraceReader};
use differential_dataflow::Data;
use timely::container::PushInto;
use timely::dataflow::Scope;
use timely::Container;

use crate::extensions::arrange::ArrangementSize;

/// Extension trait for the `reduce_abelian` differential dataflow method.
pub(crate) trait MzReduce<G: Scope, T1: TraceReader<Time = G::Timestamp>>
where
    G::Timestamp: Lattice + Ord,
{
    /// Applies `reduce` to arranged data, and returns an arrangement of output data.
    fn mz_reduce_abelian<L, K, V, Bu, T2>(
        &self,
        name: &str,
        logic: L,
    ) -> Arranged<G, TraceAgent<T2>>
    where
        T2: for<'a> Trace<Key<'a> = T1::Key<'a>, Time = G::Timestamp> + 'static,
        K: Data,
        V: Data,
        for<'a> T1::Key<'a>: IntoOwned<'a, Owned = K>,
        for<'a> T2::Val<'a>: IntoOwned<'a, Owned = V>,
        T2::Diff: Abelian,
        T2::Batch: Batch,
        Bu: Builder<Time = G::Timestamp, Output = T2::Batch>,
        Bu::Input: Container + PushInto<((K, V), T2::Time, T2::Diff)>,
        L: FnMut(T1::Key<'_>, &[(T1::Val<'_>, T1::Diff)], &mut Vec<(V, T2::Diff)>) + 'static,
        Arranged<G, TraceAgent<T2>>: ArrangementSize;
}

impl<G, T1> MzReduce<G, T1> for Arranged<G, T1>
where
    G::Timestamp: Lattice + Ord,
    G: Scope,
    T1: TraceReader<Time = G::Timestamp> + Clone + 'static,
    T1::Diff: Semigroup,
{
    /// Applies `reduce` to arranged data, and returns an arrangement of output data.
    fn mz_reduce_abelian<L, K, V, Bu, T2>(
        &self,
        name: &str,
        logic: L,
    ) -> Arranged<G, TraceAgent<T2>>
    where
        T2: for<'a> Trace<Key<'a> = T1::Key<'a>, Time = G::Timestamp> + 'static,
        K: Data,
        V: Data,
        for<'a> T1::Key<'a>: IntoOwned<'a, Owned = K>,
        for<'a> T2::Val<'a>: IntoOwned<'a, Owned = V>,
        T2::Diff: Abelian,
        T2::Batch: Batch,
        Bu: Builder<Time = G::Timestamp, Output = T2::Batch>,
        Bu::Input: Container + PushInto<((K, V), T2::Time, T2::Diff)>,
        L: FnMut(T1::Key<'_>, &[(T1::Val<'_>, T1::Diff)], &mut Vec<(V, T2::Diff)>) + 'static,
        Arranged<G, TraceAgent<T2>>: ArrangementSize,
    {
        // Allow access to `reduce_abelian` since we're within Mz's wrapper and force arrangement size logging.
        #[allow(clippy::disallowed_methods)]
        Arranged::<_, _>::reduce_abelian::<_, _, _, Bu, T2>(self, name, logic)
            .log_arrangement_size()
    }
}

/// Extension trait for `ReduceCore`, currently providing a reduction based
/// on an operator-pair approach.
pub trait ReduceExt<G: Scope, Tr: TraceReader<Time = G::Timestamp>>
where
    G::Timestamp: Lattice + Ord,
{
    /// This method produces a reduction pair based on the same input arrangement. Each reduction
    /// in the pair operates with its own logic and the two output arrangements from the reductions
    /// are produced as a result. The method is useful for reductions that need to present different
    /// output views on the same input data. An example is producing an error-free reduction output
    /// along with a separate error output indicating when the error-free output is valid.
    fn reduce_pair<L1, K, V1, Bu1, T1, L2, V2, Bu2, T2>(
        &self,
        name1: &str,
        name2: &str,
        logic1: L1,
        logic2: L2,
    ) -> (Arranged<G, TraceAgent<T1>>, Arranged<G, TraceAgent<T2>>)
    where
        K: Data,
        T1: Trace + for<'a> TraceReader<Key<'a> = Tr::Key<'a>, Time = G::Timestamp> + 'static,
        for<'a> T1::Key<'a>: IntoOwned<'a, Owned = K>,
        for<'a> T1::Val<'a>: IntoOwned<'a, Owned = V1>,
        T1::Diff: Abelian,
        T1::Batch: Batch,
        Bu1: Builder<Time = G::Timestamp, Output = T1::Batch>,
        Bu1::Input: Container + PushInto<((K, V1), T1::Time, T1::Diff)>,
        L1: FnMut(Tr::Key<'_>, &[(Tr::Val<'_>, Tr::Diff)], &mut Vec<(V1, T1::Diff)>) + 'static,
        V1: Data,
        T2: Trace + for<'a> TraceReader<Key<'a> = Tr::Key<'a>, Time = G::Timestamp> + 'static,
        for<'a> T2::Val<'a>: IntoOwned<'a, Owned = V2>,
        for<'a> T2::Val<'a>: IntoOwned<'a, Owned = V2>,
        T2::Diff: Abelian,
        T2::Batch: Batch,
        Bu2: Builder<Time = G::Timestamp, Output = T2::Batch>,
        Bu2::Input: Container + PushInto<((K, V2), T2::Time, T2::Diff)>,
        L2: FnMut(Tr::Key<'_>, &[(Tr::Val<'_>, Tr::Diff)], &mut Vec<(V2, T2::Diff)>) + 'static,
        V2: Data,
        Arranged<G, TraceAgent<T1>>: ArrangementSize,
        Arranged<G, TraceAgent<T2>>: ArrangementSize;
}

impl<G: Scope, Tr> ReduceExt<G, Tr> for Arranged<G, Tr>
where
    G::Timestamp: Lattice + Ord,
    Tr: TraceReader<Time = G::Timestamp> + Clone + 'static,
    Tr::Diff: Semigroup,
{
    fn reduce_pair<L1, K, V1, Bu1, T1, L2, V2, Bu2, T2>(
        &self,
        name1: &str,
        name2: &str,
        logic1: L1,
        logic2: L2,
    ) -> (Arranged<G, TraceAgent<T1>>, Arranged<G, TraceAgent<T2>>)
    where
        K: Data,
        T1: Trace + for<'a> TraceReader<Key<'a> = Tr::Key<'a>, Time = G::Timestamp> + 'static,
        for<'a> T1::Key<'a>: IntoOwned<'a, Owned = K>,
        for<'a> T1::Val<'a>: IntoOwned<'a, Owned = V1>,
        T1::Diff: Abelian,
        T1::Batch: Batch,
        Bu1: Builder<Time = G::Timestamp, Output = T1::Batch>,
        Bu1::Input: Container + PushInto<((K, V1), T1::Time, T1::Diff)>,
        L1: FnMut(Tr::Key<'_>, &[(Tr::Val<'_>, Tr::Diff)], &mut Vec<(V1, T1::Diff)>) + 'static,
        V1: Data,
        T2: Trace + for<'a> TraceReader<Key<'a> = Tr::Key<'a>, Time = G::Timestamp> + 'static,
        for<'a> T2::Val<'a>: IntoOwned<'a, Owned = V2>,
        for<'a> T2::Val<'a>: IntoOwned<'a, Owned = V2>,
        T2::Diff: Abelian,
        T2::Batch: Batch,
        Bu2: Builder<Time = G::Timestamp, Output = T2::Batch>,
        Bu2::Input: Container + PushInto<((K, V2), T2::Time, T2::Diff)>,
        L2: FnMut(Tr::Key<'_>, &[(Tr::Val<'_>, Tr::Diff)], &mut Vec<(V2, T2::Diff)>) + 'static,
        V2: Data,
        Arranged<G, TraceAgent<T1>>: ArrangementSize,
        Arranged<G, TraceAgent<T2>>: ArrangementSize,
    {
        let arranged1 = self.mz_reduce_abelian::<L1, _, _, Bu1, T1>(name1, logic1);
        let arranged2 = self.mz_reduce_abelian::<L2, _, _, Bu2, T2>(name2, logic2);
        (arranged1, arranged2)
    }
}
