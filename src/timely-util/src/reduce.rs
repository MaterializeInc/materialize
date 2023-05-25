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
use differential_dataflow::operators::reduce::ReduceCore;
use differential_dataflow::trace::{Batch, Trace, TraceReader};
use differential_dataflow::Data;
use timely::dataflow::Scope;

/// Extension trait for the `reduce_core` differential dataflow method.
pub trait MzReduce<G: Scope, K: Data, V: Data, R: Semigroup>: ReduceCore<G, K, V, R>
where
    G::Timestamp: Lattice + Ord,
{
    /// Applies `reduce` to arranged data, and returns an arrangement of output data.
    fn mz_reduce_abelian<L, T2>(&self, name: &str, mut logic: L) -> Arranged<G, TraceAgent<T2>>
    where
        T2: Trace + TraceReader<Key = K, Time = G::Timestamp> + 'static,
        T2::Val: Data,
        T2::R: Abelian,
        T2::Batch: Batch,
        L: FnMut(&K, &[(&V, R)], &mut Vec<(T2::Val, T2::R)>) + 'static,
    {
        // Allow access to `reduce_core` since we're within Mz's wrapper.
        #[allow(clippy::disallowed_methods)]
        self.reduce_core::<_, T2>(name, move |key, input, output, change| {
            if !input.is_empty() {
                logic(key, input, change);
            }
            change.extend(output.drain(..).map(|(x, d)| (x, d.negate())));
        })
    }
}

impl<G, K, V, T1, R> MzReduce<G, K, V, R> for Arranged<G, T1>
where
    G::Timestamp: Lattice + Ord,
    G: Scope,
    K: Data,
    V: Data,
    R: Semigroup,
    T1: TraceReader<Key = K, Val = V, Time = G::Timestamp, R = R> + Clone + 'static,
{
}

/// Extension trait for `ReduceCore`, currently providing a reduction based
/// on an operator-pair approach.
pub trait ReduceExt<G: Scope, K: Data, V: Data, R: Semigroup>
where
    G::Timestamp: Lattice + Ord,
{
    /// This method produces a reduction pair based on the same input arrangement. Each reduction
    /// in the pair operates with its own logic and the two output arrangements from the reductions
    /// are produced as a result. The method is useful for reductions that need to present different
    /// output views on the same input data. An example is producing an error-free reduction output
    /// along with a separate error output indicating when the error-free output is valid.
    fn reduce_pair<L1, T1, L2, T2>(
        &self,
        name1: &str,
        name2: &str,
        logic1: L1,
        logic2: L2,
    ) -> (Arranged<G, TraceAgent<T1>>, Arranged<G, TraceAgent<T2>>)
    where
        T1: Trace + TraceReader<Key = K, Time = G::Timestamp> + 'static,
        T1::Val: Data,
        T1::R: Abelian,
        T1::Batch: Batch,
        L1: FnMut(&K, &[(&V, R)], &mut Vec<(T1::Val, T1::R)>) + 'static,
        T2: Trace + TraceReader<Key = K, Time = G::Timestamp> + 'static,
        T2::Val: Data,
        T2::R: Abelian,
        T2::Batch: Batch,
        L2: FnMut(&K, &[(&V, R)], &mut Vec<(T2::Val, T2::R)>) + 'static;
}

impl<G: Scope, K: Data, V: Data, Tr, R: Semigroup> ReduceExt<G, K, V, R> for Arranged<G, Tr>
where
    G::Timestamp: Lattice + Ord,
    Tr: TraceReader<Key = K, Val = V, Time = G::Timestamp, R = R> + Clone + 'static,
{
    fn reduce_pair<L1, T1, L2, T2>(
        &self,
        name1: &str,
        name2: &str,
        logic1: L1,
        logic2: L2,
    ) -> (Arranged<G, TraceAgent<T1>>, Arranged<G, TraceAgent<T2>>)
    where
        T1: Trace + TraceReader<Key = K, Time = G::Timestamp> + 'static,
        T1::Val: Data,
        T1::R: Abelian,
        T1::Batch: Batch,
        L1: FnMut(&K, &[(&V, R)], &mut Vec<(T1::Val, T1::R)>) + 'static,
        T2: Trace + TraceReader<Key = K, Time = G::Timestamp> + 'static,
        T2::Val: Data,
        T2::R: Abelian,
        T2::Batch: Batch,
        L2: FnMut(&K, &[(&V, R)], &mut Vec<(T2::Val, T2::R)>) + 'static,
    {
        let arranged1 = self.mz_reduce_abelian::<L1, T1>(name1, logic1);
        let arranged2 = self.mz_reduce_abelian::<L2, T2>(name2, logic2);
        (arranged1, arranged2)
    }
}
