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
use differential_dataflow::trace::implementations::{BatchContainer, LayoutExt};
use differential_dataflow::trace::{Builder, Trace, TraceReader};
use timely::Container;
use timely::container::PushInto;
use timely::progress::Timestamp;

use crate::extensions::arrange::ArrangementSize;

/// Owned key type of a [`TraceReader`]; replaces the old `Tr::KeyOwn` associated type.
type KeyOwn<Tr> = <<Tr as LayoutExt>::KeyContainer as BatchContainer>::Owned;

/// Extension trait for the `reduce_abelian` differential dataflow method.
pub(crate) trait MzReduce<T, T1>
where
    T: Timestamp + Lattice,
    T1: TraceReader<Time = T>,
{
    /// Applies `reduce` to arranged data, and returns an arrangement of output data.
    fn mz_reduce_abelian<L, Bu, T2>(self, name: &str, logic: L) -> Arranged<TraceAgent<T2>>
    where
        T2: for<'a> Trace<Key<'a> = T1::Key<'a>, ValOwn: Data, Time = T, Diff: Abelian> + 'static,
        Bu: Builder<Time = T, Output = T2::Batch>,
        Bu::Input:
            Container + InternalMerge + PushInto<((KeyOwn<T1>, T2::ValOwn), T2::Time, T2::Diff)>,
        L: FnMut(T1::Key<'_>, &[(T1::Val<'_>, T1::Diff)], &mut Vec<(T2::ValOwn, T2::Diff)>)
            + 'static,
        Arranged<TraceAgent<T2>>: ArrangementSize;
}

impl<T, T1> MzReduce<T, T1> for Arranged<T1>
where
    T: Timestamp + Lattice,
    T1: TraceReader<Time = T> + Clone + 'static,
    KeyOwn<T1>: Ord + Clone,
{
    /// Applies `reduce` to arranged data, and returns an arrangement of output data.
    fn mz_reduce_abelian<L, Bu, T2>(self, name: &str, logic: L) -> Arranged<TraceAgent<T2>>
    where
        T2: for<'a> Trace<Key<'a> = T1::Key<'a>, ValOwn: Data, Time = T, Diff: Abelian> + 'static,
        Bu: Builder<
                Time = T,
                Input: Container
                           + InternalMerge
                           + PushInto<((KeyOwn<T1>, T2::ValOwn), T2::Time, T2::Diff)>,
                Output = T2::Batch,
            >,
        L: FnMut(T1::Key<'_>, &[(T1::Val<'_>, T1::Diff)], &mut Vec<(T2::ValOwn, T2::Diff)>)
            + 'static,
        Arranged<TraceAgent<T2>>: ArrangementSize,
    {
        // Allow access to `reduce_abelian` since we're within Mz's wrapper and force arrangement size logging.
        #[allow(clippy::disallowed_methods)]
        Arranged::<_>::reduce_abelian::<_, Bu, T2, _>(self, name, logic, |buf, key, updates| {
            let key_owned: KeyOwn<T1> = <T1::KeyContainer as BatchContainer>::into_owned(key);
            for (val, time, diff) in updates.drain(..) {
                buf.push_into(((key_owned.clone(), val), time, diff));
            }
        })
        .log_arrangement_size()
    }
}

/// Extension trait for `ReduceCore`, currently providing a reduction based
/// on an operator-pair approach.
pub trait ReduceExt<T, Tr>
where
    T: Timestamp + Lattice,
    Tr: TraceReader<Time = T>,
{
    /// This method produces a reduction pair based on the same input arrangement. Each reduction
    /// in the pair operates with its own logic and the two output arrangements from the reductions
    /// are produced as a result. The method is useful for reductions that need to present different
    /// output views on the same input data. An example is producing an error-free reduction output
    /// along with a separate error output indicating when the error-free output is valid.
    fn reduce_pair<L1, Bu1, T1, L2, Bu2, T2>(
        self,
        name1: &str,
        name2: &str,
        logic1: L1,
        logic2: L2,
    ) -> (Arranged<TraceAgent<T1>>, Arranged<TraceAgent<T2>>)
    where
        T1: for<'a> Trace<Key<'a> = Tr::Key<'a>, ValOwn: Data, Time = T, Diff: Abelian> + 'static,
        Bu1: Builder<
                Time = T,
                Input: Container
                           + InternalMerge
                           + PushInto<((KeyOwn<Tr>, T1::ValOwn), T1::Time, T1::Diff)>,
                Output = T1::Batch,
            >,
        L1: FnMut(Tr::Key<'_>, &[(Tr::Val<'_>, Tr::Diff)], &mut Vec<(T1::ValOwn, T1::Diff)>)
            + 'static,
        T2: for<'a> Trace<Key<'a> = Tr::Key<'a>, ValOwn: Data, Time = T, Diff: Abelian> + 'static,
        Bu2: Builder<
                Time = T,
                Input: Container
                           + InternalMerge
                           + PushInto<((KeyOwn<Tr>, T2::ValOwn), T2::Time, T2::Diff)>,
                Output = T2::Batch,
            >,
        L2: FnMut(Tr::Key<'_>, &[(Tr::Val<'_>, Tr::Diff)], &mut Vec<(T2::ValOwn, T2::Diff)>)
            + 'static,
        Arranged<TraceAgent<T1>>: ArrangementSize,
        Arranged<TraceAgent<T2>>: ArrangementSize;
}

impl<T, Tr> ReduceExt<T, Tr> for Arranged<Tr>
where
    T: Timestamp + Lattice,
    Tr: TraceReader<Time = T> + Clone + 'static,
    KeyOwn<Tr>: Ord + Clone,
{
    fn reduce_pair<L1, Bu1, T1, L2, Bu2, T2>(
        self,
        name1: &str,
        name2: &str,
        logic1: L1,
        logic2: L2,
    ) -> (Arranged<TraceAgent<T1>>, Arranged<TraceAgent<T2>>)
    where
        T1: for<'a> Trace<Key<'a> = Tr::Key<'a>, ValOwn: Data, Time = T, Diff: Abelian> + 'static,
        Bu1: Builder<
                Time = T,
                Input: Container
                           + InternalMerge
                           + PushInto<((KeyOwn<Tr>, T1::ValOwn), T1::Time, T1::Diff)>,
                Output = T1::Batch,
            >,
        L1: FnMut(Tr::Key<'_>, &[(Tr::Val<'_>, Tr::Diff)], &mut Vec<(T1::ValOwn, T1::Diff)>)
            + 'static,
        T2: for<'a> Trace<Key<'a> = Tr::Key<'a>, ValOwn: Data, Time = T, Diff: Abelian> + 'static,
        Bu2: Builder<
                Time = T,
                Input: Container
                           + InternalMerge
                           + PushInto<((KeyOwn<Tr>, T2::ValOwn), T2::Time, T2::Diff)>,
                Output = T2::Batch,
            >,
        L2: FnMut(Tr::Key<'_>, &[(Tr::Val<'_>, Tr::Diff)], &mut Vec<(T2::ValOwn, T2::Diff)>)
            + 'static,
        Arranged<TraceAgent<T1>>: ArrangementSize,
        Arranged<TraceAgent<T2>>: ArrangementSize,
    {
        let arranged1 = self.clone().mz_reduce_abelian::<L1, Bu1, T1>(name1, logic1);
        let arranged2 = self.mz_reduce_abelian::<L2, Bu2, T2>(name2, logic2);
        (arranged1, arranged2)
    }
}
