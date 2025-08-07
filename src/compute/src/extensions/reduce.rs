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
    fn mz_reduce_abelian<L, Bu, T2>(&self, name: &str, logic: L) -> Arranged<G, TraceAgent<T2>>
    where
        T2: for<'a> Trace<
                Key<'a> = T1::Key<'a>,
                KeyOwn = T1::KeyOwn,
                ValOwn: Data,
                Time = G::Timestamp,
                Diff: Abelian,
            > + 'static,
        Bu: Builder<Time = G::Timestamp, Output = T2::Batch>,
        Bu::Input: Container + PushInto<((T1::KeyOwn, T2::ValOwn), T2::Time, T2::Diff)>,
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
    fn mz_reduce_abelian<L, Bu, T2>(&self, name: &str, logic: L) -> Arranged<G, TraceAgent<T2>>
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
                Input: Container + PushInto<((T1::KeyOwn, T2::ValOwn), T2::Time, T2::Diff)>,
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

/// Extension trait for `ReduceCore`, currently providing a reduction based
/// on an operator-pair approach.
pub trait ReduceExt<G, Tr>
where
    G: Scope,
    G::Timestamp: Lattice,
    Tr: TraceReader<Time = G::Timestamp>,
{
    /// This method produces a reduction pair based on the same input arrangement. Each reduction
    /// in the pair operates with its own logic and the two output arrangements from the reductions
    /// are produced as a result. The method is useful for reductions that need to present different
    /// output views on the same input data. An example is producing an error-free reduction output
    /// along with a separate error output indicating when the error-free output is valid.
    fn reduce_pair<L1, Bu1, T1, L2, Bu2, T2>(
        &self,
        name1: &str,
        name2: &str,
        logic1: L1,
        logic2: L2,
    ) -> (Arranged<G, TraceAgent<T1>>, Arranged<G, TraceAgent<T2>>)
    where
        T1: for<'a> Trace<
                Key<'a> = Tr::Key<'a>,
                KeyOwn = Tr::KeyOwn,
                ValOwn: Data,
                Time = G::Timestamp,
                Diff: Abelian,
            > + 'static,
        Bu1: Builder<
                Time = G::Timestamp,
                Input: Container + PushInto<((T1::KeyOwn, T1::ValOwn), T1::Time, T1::Diff)>,
                Output = T1::Batch,
            >,
        L1: FnMut(Tr::Key<'_>, &[(Tr::Val<'_>, Tr::Diff)], &mut Vec<(T1::ValOwn, T1::Diff)>)
            + 'static,
        T2: for<'a> Trace<
                Key<'a> = Tr::Key<'a>,
                KeyOwn = Tr::KeyOwn,
                ValOwn: Data,
                Time = G::Timestamp,
                Diff: Abelian,
            > + 'static,
        Bu2: Builder<
                Time = G::Timestamp,
                Input: Container + PushInto<((T1::KeyOwn, T2::ValOwn), T2::Time, T2::Diff)>,
                Output = T2::Batch,
            >,
        L2: FnMut(Tr::Key<'_>, &[(Tr::Val<'_>, Tr::Diff)], &mut Vec<(T2::ValOwn, T2::Diff)>)
            + 'static,
        Arranged<G, TraceAgent<T1>>: ArrangementSize,
        Arranged<G, TraceAgent<T2>>: ArrangementSize;
}

impl<G, Tr> ReduceExt<G, Tr> for Arranged<G, Tr>
where
    G: Scope,
    G::Timestamp: Lattice,
    Tr: TraceReader<Time = G::Timestamp, KeyOwn: Ord> + Clone + 'static,
{
    fn reduce_pair<L1, Bu1, T1, L2, Bu2, T2>(
        &self,
        name1: &str,
        name2: &str,
        logic1: L1,
        logic2: L2,
    ) -> (Arranged<G, TraceAgent<T1>>, Arranged<G, TraceAgent<T2>>)
    where
        T1: for<'a> Trace<
                Key<'a> = Tr::Key<'a>,
                KeyOwn = Tr::KeyOwn,
                ValOwn: Data,
                Time = G::Timestamp,
                Diff: Abelian,
            > + 'static,
        Bu1: Builder<
                Time = G::Timestamp,
                Input: Container + PushInto<((T1::KeyOwn, T1::ValOwn), T1::Time, T1::Diff)>,
                Output = T1::Batch,
            >,
        L1: FnMut(Tr::Key<'_>, &[(Tr::Val<'_>, Tr::Diff)], &mut Vec<(T1::ValOwn, T1::Diff)>)
            + 'static,
        T2: for<'a> Trace<
                Key<'a> = Tr::Key<'a>,
                KeyOwn = Tr::KeyOwn,
                ValOwn: Data,
                Time = G::Timestamp,
                Diff: Abelian,
            > + 'static,
        Bu2: Builder<
                Time = G::Timestamp,
                Input: Container + PushInto<((T1::KeyOwn, T2::ValOwn), T2::Time, T2::Diff)>,
                Output = T2::Batch,
            >,
        L2: FnMut(Tr::Key<'_>, &[(Tr::Val<'_>, Tr::Diff)], &mut Vec<(T2::ValOwn, T2::Diff)>)
            + 'static,
        Arranged<G, TraceAgent<T1>>: ArrangementSize,
        Arranged<G, TraceAgent<T2>>: ArrangementSize,
    {
        let arranged1 = self.mz_reduce_abelian::<L1, Bu1, T1>(name1, logic1);
        let arranged2 = self.mz_reduce_abelian::<L2, Bu2, T2>(name2, logic2);
        (arranged1, arranged2)
    }
}
