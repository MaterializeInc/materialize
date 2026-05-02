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

use columnation::Columnation;
use differential_dataflow::Data;
use differential_dataflow::difference::Abelian;
use differential_dataflow::operators::arrange::{Arranged, TraceAgent};
use differential_dataflow::trace::implementations::{BatchContainer, LayoutExt};
use differential_dataflow::trace::{Builder, Trace, TraceReader};
use mz_timely_util::columnation::ColumnationStack;
use timely::Container;
use timely::container::PushInto;

use crate::extensions::arrange::ArrangementSize;

type KeyOwn<Tr> = <<Tr as LayoutExt>::KeyContainer as BatchContainer>::Owned;

pub trait ClearContainer {
    fn clear(&mut self);
}

impl<T> ClearContainer for Vec<T> {
    fn clear(&mut self) {
        Vec::clear(self)
    }
}

impl<D, T, R> ClearContainer for ColumnationStack<(D, T, R)>
where
    D: Columnation + Clone + 'static,
    T: Columnation + Clone + 'static,
    R: Columnation + Clone + 'static,
{
    fn clear(&mut self) {
        ColumnationStack::clear(self)
    }
}

/// Extension trait for the `reduce_abelian` differential dataflow method.
pub(crate) trait MzReduce<'scope, T1: TraceReader> {
    /// Applies `reduce` to arranged data, and returns an arrangement of output data.
    fn mz_reduce_abelian<L, Bu, T2>(self, name: &str, logic: L) -> Arranged<'scope, TraceAgent<T2>>
    where
        T2: for<'a> Trace<Key<'a> = T1::Key<'a>, ValOwn: Data, Time = T1::Time, Diff: Abelian>
            + 'static,
        Bu: Builder<Time = T1::Time, Output = T2::Batch>,
        Bu::Input: Container
            + Default
            + ClearContainer
            + PushInto<((KeyOwn<T1>, T2::ValOwn), T2::Time, T2::Diff)>,
        L: FnMut(T1::Key<'_>, &[(T1::Val<'_>, T1::Diff)], &mut Vec<(T2::ValOwn, T2::Diff)>)
            + 'static,
        Arranged<'scope, TraceAgent<T2>>: ArrangementSize;
}

impl<'scope, T1> MzReduce<'scope, T1> for Arranged<'scope, T1>
where
    T1: TraceReader + Clone + 'static,
{
    /// Applies `reduce` to arranged data, and returns an arrangement of output data.
    fn mz_reduce_abelian<L, Bu, T2>(self, name: &str, logic: L) -> Arranged<'scope, TraceAgent<T2>>
    where
        T2: for<'a> Trace<Key<'a> = T1::Key<'a>, ValOwn: Data, Time = T1::Time, Diff: Abelian>
            + 'static,
        Bu: Builder<
                Time = T1::Time,
                Input: Container
                           + Default
                           + ClearContainer
                           + PushInto<((KeyOwn<T1>, T2::ValOwn), T2::Time, T2::Diff)>,
                Output = T2::Batch,
            >,
        L: FnMut(T1::Key<'_>, &[(T1::Val<'_>, T1::Diff)], &mut Vec<(T2::ValOwn, T2::Diff)>)
            + 'static,
        Arranged<'scope, TraceAgent<T2>>: ArrangementSize,
    {
        // Construct a push closure for `reduce_abelian`.
        use differential_dataflow::trace::implementations::BatchContainer;
        let push_closure =
            |buf: &mut Bu::Input,
             key: T1::Key<'_>,
             updates: &mut Vec<(T2::ValOwn, T2::Time, T2::Diff)>| {
                buf.clear();
                let key_owned = <T1::KeyContainer as BatchContainer>::into_owned(key);
                for (val, time, diff) in updates.drain(..) {
                    buf.push_into(((key_owned.clone(), val), time, diff));
                }
            };

        // Allow access to `reduce_abelian` since we're within Mz's wrapper and force arrangement size logging.
        #[allow(clippy::disallowed_methods)]
        Arranged::<_>::reduce_abelian::<_, Bu, T2, _>(self, name, logic, push_closure)
            .log_arrangement_size()
    }
}
