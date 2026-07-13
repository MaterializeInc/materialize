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
use differential_dataflow::difference::Semigroup;
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::trace::cursor::BatchCursor;
use differential_dataflow::trace::implementations::BatchContainer;
use differential_dataflow::trace::{Builder, Cursor};
use mz_repr::{Diff, Row};
use mz_row_spine::{DatumSeq, RowBuilder, RowRowBuilder, RowSpine};
use timely::container::PushInto;

use mz_timely_util::columnation::ColumnationStack;

use crate::extensions::arrange::ArrangementSize;
use crate::render::errors::DataflowErrorSer;
use crate::typedefs::{
    ErrAgent, ErrBuilder, ErrSpine, MzTimestamp, RowAgent, RowErrBuilder, RowErrSpine, RowRowAgent,
    RowRowSpine, RowValAgent,
};

/// Clears a builder input container between keys in the reduce push closure.
///
/// The reduce operator has no way to reset the caller's input container itself, so the push
/// closure clears it before staging each key's updates. Failing to clear leaks one key's rows
/// into the next.
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

/// Drains `updates` into the builder input `buf`, prepending `key_owned` to each value.
///
/// Clears `buf` first because the reduce operator cannot reset it between keys, and failing to
/// clear leaks one key's rows into the next.
pub(crate) fn push_reduced<C, K, V, T>(buf: &mut C, key_owned: &K, updates: &mut Vec<(V, T, Diff)>)
where
    C: ClearContainer + PushInto<((K, V), T, Diff)>,
    K: Clone,
{
    ClearContainer::clear(buf);
    for (val, time, diff) in updates.drain(..) {
        PushInto::push_into(buf, ((key_owned.clone(), val), time, diff));
    }
}

// `differential`'s `reduce_abelian` carries a higher-ranked output-key bound
// (`for<'a> Key<'a> = BatchKey<'a, Tr1>`) that the compiler only normalizes when the input and
// output trace types are concrete through a function signature. Inline calls (e.g. through a
// macro) leave `Tr1`/`Tr2` as inference variables and fail the projection. The helpers below fix
// the concrete spine structures in their signatures, generic only over lifetime, timestamp, input
// diff, and logic, which keeps the projection normalizable. Each covers one distinct
// (input-spine-structure, output-spine-structure) combination and is reused across call sites.

/// `reduce_abelian` from a `RowRow` input arrangement into a `RowRow` output arrangement.
pub(crate) fn reduce_row_row_to_row_row<'s, T, L>(
    arr: Arranged<'s, RowRowAgent<T, Diff>>,
    name: &str,
    logic: L,
) -> Arranged<'s, RowRowAgent<T, Diff>>
where
    T: MzTimestamp,
    L: FnMut(DatumSeq<'_>, &[(DatumSeq<'_>, Diff)], &mut Vec<(Row, Diff)>) + 'static,
{
    let push = |buf: &mut <RowRowBuilder<T, Diff> as Builder>::Input,
                key: DatumSeq<'_>,
                updates: &mut Vec<(Row, T, Diff)>| {
        let key_owned: Row = <<BatchCursor<RowRowAgent<T, Diff>> as Cursor>::KeyContainer as BatchContainer>::into_owned(key);
        push_reduced(buf, &key_owned, updates);
    };
    #[allow(clippy::disallowed_methods)]
    arr.reduce_abelian::<_, RowRowBuilder<T, Diff>, RowRowSpine<T, Diff>, _>(name, logic, push)
        .log_arrangement_size()
}

/// `reduce_abelian` from a `RowRow` input arrangement into a `RowErr` output arrangement.
pub(crate) fn reduce_row_row_to_row_err<'s, T, L>(
    arr: Arranged<'s, RowRowAgent<T, Diff>>,
    name: &str,
    logic: L,
) -> Arranged<'s, RowValAgent<DataflowErrorSer, T, Diff>>
where
    T: MzTimestamp,
    L: FnMut(DatumSeq<'_>, &[(DatumSeq<'_>, Diff)], &mut Vec<(DataflowErrorSer, Diff)>) + 'static,
{
    let push = |buf: &mut <RowErrBuilder<T, Diff> as Builder>::Input,
                key: DatumSeq<'_>,
                updates: &mut Vec<(DataflowErrorSer, T, Diff)>| {
        let key_owned: Row = <<BatchCursor<RowRowAgent<T, Diff>> as Cursor>::KeyContainer as BatchContainer>::into_owned(key);
        push_reduced(buf, &key_owned, updates);
    };
    #[allow(clippy::disallowed_methods)]
    arr.reduce_abelian::<_, RowErrBuilder<T, Diff>, RowErrSpine<T, Diff>, _>(name, logic, push)
        .log_arrangement_size()
}

/// `reduce_abelian` from a `RowVal` input arrangement into a `RowRow` output arrangement.
pub(crate) fn reduce_row_val_to_row_row<'s, T, V, L>(
    arr: Arranged<'s, RowValAgent<V, T, Diff>>,
    name: &str,
    logic: L,
) -> Arranged<'s, RowRowAgent<T, Diff>>
where
    T: MzTimestamp,
    V: Data + Columnation,
    L: FnMut(DatumSeq<'_>, &[(&V, Diff)], &mut Vec<(Row, Diff)>) + 'static,
{
    let push = |buf: &mut <RowRowBuilder<T, Diff> as Builder>::Input,
                key: DatumSeq<'_>,
                updates: &mut Vec<(Row, T, Diff)>| {
        let key_owned: Row = <<BatchCursor<RowValAgent<V, T, Diff>> as Cursor>::KeyContainer as BatchContainer>::into_owned(key);
        push_reduced(buf, &key_owned, updates);
    };
    #[allow(clippy::disallowed_methods)]
    arr.reduce_abelian::<_, RowRowBuilder<T, Diff>, RowRowSpine<T, Diff>, _>(name, logic, push)
        .log_arrangement_size()
}

/// `reduce_abelian` from a `RowVal` input arrangement into a `RowErr` output arrangement.
pub(crate) fn reduce_row_val_to_row_err<'s, T, V, L>(
    arr: Arranged<'s, RowValAgent<V, T, Diff>>,
    name: &str,
    logic: L,
) -> Arranged<'s, RowValAgent<DataflowErrorSer, T, Diff>>
where
    T: MzTimestamp,
    V: Data + Columnation,
    L: FnMut(DatumSeq<'_>, &[(&V, Diff)], &mut Vec<(DataflowErrorSer, Diff)>) + 'static,
{
    let push = |buf: &mut <RowErrBuilder<T, Diff> as Builder>::Input,
                key: DatumSeq<'_>,
                updates: &mut Vec<(DataflowErrorSer, T, Diff)>| {
        let key_owned: Row = <<BatchCursor<RowValAgent<V, T, Diff>> as Cursor>::KeyContainer as BatchContainer>::into_owned(key);
        push_reduced(buf, &key_owned, updates);
    };
    #[allow(clippy::disallowed_methods)]
    arr.reduce_abelian::<_, RowErrBuilder<T, Diff>, RowErrSpine<T, Diff>, _>(name, logic, push)
        .log_arrangement_size()
}

/// `reduce_abelian` from a `Row` (unit-valued) input arrangement into a `RowRow` output
/// arrangement. Generic over the input diff `R`.
pub(crate) fn reduce_row_to_row_row<'s, T, R, L>(
    arr: Arranged<'s, RowAgent<T, R>>,
    name: &str,
    logic: L,
) -> Arranged<'s, RowRowAgent<T, Diff>>
where
    T: MzTimestamp,
    R: Semigroup + Data + Columnation,
    L: FnMut(DatumSeq<'_>, &[(&(), R)], &mut Vec<(Row, Diff)>) + 'static,
{
    let push = |buf: &mut <RowRowBuilder<T, Diff> as Builder>::Input,
                key: DatumSeq<'_>,
                updates: &mut Vec<(Row, T, Diff)>| {
        let key_owned: Row =
            <<BatchCursor<RowAgent<T, R>> as Cursor>::KeyContainer as BatchContainer>::into_owned(
                key,
            );
        push_reduced(buf, &key_owned, updates);
    };
    #[allow(clippy::disallowed_methods)]
    arr.reduce_abelian::<_, RowRowBuilder<T, Diff>, RowRowSpine<T, Diff>, _>(name, logic, push)
        .log_arrangement_size()
}

/// `reduce_abelian` from a `Row` (unit-valued) input arrangement into a `RowErr` output
/// arrangement. Generic over the input diff `R`.
pub(crate) fn reduce_row_to_row_err<'s, T, R, L>(
    arr: Arranged<'s, RowAgent<T, R>>,
    name: &str,
    logic: L,
) -> Arranged<'s, RowValAgent<DataflowErrorSer, T, Diff>>
where
    T: MzTimestamp,
    R: Semigroup + Data + Columnation,
    L: FnMut(DatumSeq<'_>, &[(&(), R)], &mut Vec<(DataflowErrorSer, Diff)>) + 'static,
{
    let push = |buf: &mut <RowErrBuilder<T, Diff> as Builder>::Input,
                key: DatumSeq<'_>,
                updates: &mut Vec<(DataflowErrorSer, T, Diff)>| {
        let key_owned: Row =
            <<BatchCursor<RowAgent<T, R>> as Cursor>::KeyContainer as BatchContainer>::into_owned(
                key,
            );
        push_reduced(buf, &key_owned, updates);
    };
    #[allow(clippy::disallowed_methods)]
    arr.reduce_abelian::<_, RowErrBuilder<T, Diff>, RowErrSpine<T, Diff>, _>(name, logic, push)
        .log_arrangement_size()
}

/// `reduce_abelian` from a `Row` (unit-valued) input arrangement into a `Row` (unit-valued) output
/// arrangement. Generic over the input diff `R`.
pub(crate) fn reduce_row_to_row<'s, T, R, L>(
    arr: Arranged<'s, RowAgent<T, R>>,
    name: &str,
    logic: L,
) -> Arranged<'s, RowAgent<T, Diff>>
where
    T: MzTimestamp,
    R: Semigroup + Data + Columnation,
    L: FnMut(DatumSeq<'_>, &[(&(), R)], &mut Vec<((), Diff)>) + 'static,
{
    let push = |buf: &mut <RowBuilder<T, Diff> as Builder>::Input,
                key: DatumSeq<'_>,
                updates: &mut Vec<((), T, Diff)>| {
        let key_owned: Row =
            <<BatchCursor<RowAgent<T, R>> as Cursor>::KeyContainer as BatchContainer>::into_owned(
                key,
            );
        push_reduced(buf, &key_owned, updates);
    };
    #[allow(clippy::disallowed_methods)]
    arr.reduce_abelian::<_, RowBuilder<T, Diff>, RowSpine<T, Diff>, _>(name, logic, push)
        .log_arrangement_size()
}

/// `reduce_abelian` from an `Err` (error-keyed, unit-valued) input arrangement into an `Err`
/// output arrangement.
pub(crate) fn reduce_err_to_err<'s, T, L>(
    arr: Arranged<'s, ErrAgent<T, Diff>>,
    name: &str,
    logic: L,
) -> Arranged<'s, ErrAgent<T, Diff>>
where
    T: MzTimestamp,
    L: FnMut(&DataflowErrorSer, &[(&(), Diff)], &mut Vec<((), Diff)>) + 'static,
{
    let push = |buf: &mut <ErrBuilder<T, Diff> as Builder>::Input,
                key: &DataflowErrorSer,
                updates: &mut Vec<((), T, Diff)>| {
        let key_owned: DataflowErrorSer = <<BatchCursor<ErrAgent<T, Diff>> as Cursor>::KeyContainer as BatchContainer>::into_owned(key);
        push_reduced(buf, &key_owned, updates);
    };
    #[allow(clippy::disallowed_methods)]
    arr.reduce_abelian::<_, ErrBuilder<T, Diff>, ErrSpine<T, Diff>, _>(name, logic, push)
        .log_arrangement_size()
}
