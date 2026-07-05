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
use differential_dataflow::trace::cursor::{
    BatchCursor, BatchDiff, BatchKey, BatchVal, BatchValOwn, Cursor, Navigable,
};
use differential_dataflow::trace::implementations::BatchContainer;
use differential_dataflow::trace::{Builder, Trace, TraceReader};
use mz_timely_util::columnation::ColumnationStack;
use timely::Container;
use timely::container::PushInto;

use crate::extensions::arrange::ArrangementSize;

type KeyOwn<Tr> = <<BatchCursor<Tr> as Cursor>::KeyContainer as BatchContainer>::Owned;

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
///
/// The input trace is an associated type rather than a trait parameter: predicates on
/// `Self::T1` normalize once the receiver type is known, which lets the trait solver
/// discharge the higher-ranked cursor bounds at call sites where a plain trait parameter
/// would be left unresolved.
pub(crate) trait MzReduce<'scope> {
    /// The trace of the arrangement being reduced.
    type T1: TraceReader<Batch: Navigable>;

    /// Applies `reduce` to arranged data, and returns an arrangement of output data.
    fn mz_reduce_abelian<L, Bu, T2>(self, name: &str, logic: L) -> Arranged<'scope, TraceAgent<T2>>
    where
        T2: Trace<Batch: Navigable, Time = <Self::T1 as TraceReader>::Time> + 'static,
        BatchCursor<Self::T1>: Cursor<Time = <Self::T1 as TraceReader>::Time>,
        for<'a> BatchCursor<T2>:
            Cursor<Key<'a> = BatchKey<'a, Self::T1>, ValOwn: Data, Time = T2::Time, Diff: Abelian>,
        Bu: Builder<Time = <Self::T1 as TraceReader>::Time, Output = T2::Batch> + 'static,
        Bu::Input: Container
            + Default
            + ClearContainer
            + PushInto<((KeyOwn<Self::T1>, BatchValOwn<T2>), T2::Time, BatchDiff<T2>)>,
        L: FnMut(
                BatchKey<'_, Self::T1>,
                &[(BatchVal<'_, Self::T1>, BatchDiff<Self::T1>)],
                &mut Vec<(BatchValOwn<T2>, BatchDiff<T2>)>,
            ) + 'static,
        Arranged<'scope, TraceAgent<T2>>: ArrangementSize;
}

impl<'scope, T1> MzReduce<'scope> for Arranged<'scope, T1>
where
    T1: TraceReader<Batch: Navigable> + Clone + 'static,
{
    type T1 = T1;

    /// Applies `reduce` to arranged data, and returns an arrangement of output data.
    fn mz_reduce_abelian<L, Bu, T2>(self, name: &str, logic: L) -> Arranged<'scope, TraceAgent<T2>>
    where
        T2: Trace<Batch: Navigable, Time = T1::Time> + 'static,
        BatchCursor<T1>: Cursor<Time = T1::Time>,
        for<'a> BatchCursor<T2>:
            Cursor<Key<'a> = BatchKey<'a, T1>, ValOwn: Data, Time = T2::Time, Diff: Abelian>,
        Bu: Builder<
                Time = T1::Time,
                Input: Container
                           + Default
                           + ClearContainer
                           + PushInto<((KeyOwn<T1>, BatchValOwn<T2>), T2::Time, BatchDiff<T2>)>,
                Output = T2::Batch,
            > + 'static,
        L: FnMut(
                BatchKey<'_, T1>,
                &[(BatchVal<'_, T1>, BatchDiff<T1>)],
                &mut Vec<(BatchValOwn<T2>, BatchDiff<T2>)>,
            ) + 'static,
        Arranged<'scope, TraceAgent<T2>>: ArrangementSize,
    {
        // Construct a push closure for `reduce_abelian`.
        use differential_dataflow::trace::implementations::BatchContainer;
        let push_closure =
            |buf: &mut Bu::Input,
             key: BatchKey<'_, T1>,
             updates: &mut Vec<(BatchValOwn<T2>, T2::Time, BatchDiff<T2>)>| {
                buf.clear();
                let key_owned =
                    <<BatchCursor<T1> as Cursor>::KeyContainer as BatchContainer>::into_owned(key);
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
