// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Convience typedefs for differential types.

#![allow(missing_docs)]

use differential_dataflow::operators::arrange::TraceAgent;
use differential_dataflow::trace::implementations::ord_neu::{ColKeySpine, ColValSpine};

use mz_repr::{Diff, Row, Timestamp};
use mz_storage_types::errors::DataflowError;

pub type RowRowSpine<K, V, T, R> = WrappedSpine<K, V, T, R>;

// pub type RowSpine<K, V, T, R> = ColValSpine<K, V, T, R>;
// pub type RowKeySpine<K, T, R> = ColKeySpine<K, T, R>;
// pub type ErrSpine<K, T, R> = ColKeySpine<K, T, R>;
// pub type ErrValSpine<K, T, R> = ColValSpine<K, DataflowError, T, R>;
pub type RowSpine<K, V, T, R> = WrappedSpine<K, V, T, R>;
pub type RowKeySpine<K, T, R> = WrappedSpine<K, (), T, R>;
pub type ErrSpine<K, T, R> = WrappedSpine<K, (), T, R>;
pub type ErrValSpine<K, T, R> = WrappedSpine<K, DataflowError, T, R>;

pub type TraceRowHandle<K, V, T, R> = TraceAgent<RowSpine<K, V, T, R>>;
// pub type TraceRowHandle<K, V, T, R> = TraceAgent<RowRowSpine<K, V, T, R>>;
pub type TraceKeyHandle<K, T, R> = TraceAgent<RowKeySpine<K, T, R>>;
pub type TraceErrHandle<K, T, R> = TraceAgent<ErrSpine<K, T, R>>;
pub type KeysValsHandle = TraceRowHandle<Row, Row, Timestamp, Diff>;
pub type ErrsHandle = TraceErrHandle<DataflowError, Timestamp, Diff>;

use differential_dataflow::trace::implementations::BatchContainer;

pub struct WrappedVec<T> {
    inner: Vec<T>,
}

impl<T> Default for WrappedVec<T> {
    fn default() -> Self {
        Self {
            inner: Default::default(),
        }
    }
}

#[derive(Debug)]
pub struct WrappedRef<'a, T: ?Sized> {
    pub inner: &'a T,
}

impl<'a, T: std::fmt::Display> std::fmt::Display for WrappedRef<'a, T> {
    /// Debug representation using the internal datums
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        std::fmt::Display::fmt(self.inner, f)
    }
}

// All `T: Clone` also implement `ToOwned<Owned = T>`, but without the constraint Rust
// struggles to understand why the owned type must be `T` (i.e. the one blanket impl).
impl<T: Ord + Clone + 'static> BatchContainer for WrappedVec<T> {
    type PushItem = T;
    type ReadItem<'a> = WrappedRef<'a, T>;

    fn push(&mut self, item: T) {
        self.inner.push(item);
    }
    fn copy_push(&mut self, item: &T) {
        self.inner.copy(item);
    }
    fn copy<'a>(&mut self, item: WrappedRef<'a, T>) {
        self.inner.push(item.inner.clone());
    }
    fn copy_slice(&mut self, slice: &[T]) {
        self.inner.extend_from_slice(slice);
    }
    fn copy_range(&mut self, other: &Self, start: usize, end: usize) {
        self.inner.extend_from_slice(&other.inner[start..end]);
    }
    fn with_capacity(size: usize) -> Self {
        Self {
            inner: Vec::with_capacity(size),
        }
    }
    fn reserve(&mut self, additional: usize) {
        self.inner.reserve(additional);
    }
    fn merge_capacity(cont1: &Self, cont2: &Self) -> Self {
        Self {
            inner: Vec::with_capacity(cont1.inner.len() + cont2.inner.len()),
        }
    }
    fn index(&self, index: usize) -> Self::ReadItem<'_> {
        WrappedRef {
            inner: &self.inner[index],
        }
    }
    fn len(&self) -> usize {
        self.inner[..].len()
    }
}

impl<'a, T> Copy for WrappedRef<'a, T> {}
impl<'a, T> Clone for WrappedRef<'a, T> {
    fn clone(&self) -> Self {
        *self
    }
}

use std::cmp::Ordering;
impl<'a, 'b, B: Ord> PartialEq<WrappedRef<'a, B>> for WrappedRef<'b, B> {
    fn eq(&self, other: &WrappedRef<'a, B>) -> bool {
        self.inner.eq(other.inner)
    }
}
impl<'a, B: Ord> Eq for WrappedRef<'a, B> {}
impl<'a, 'b, B: Ord> PartialOrd<WrappedRef<'a, B>> for WrappedRef<'b, B> {
    fn partial_cmp(&self, other: &WrappedRef<'a, B>) -> Option<Ordering> {
        self.inner.partial_cmp(other.inner)
    }
}
impl<'a, B: Ord> Ord for WrappedRef<'a, B> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

use differential_dataflow::trace::cursor::MyTrait;
impl<'a, T: Ord + ToOwned> MyTrait<'a> for WrappedRef<'a, T> {
    type Owned = T::Owned;
    fn into_owned(self) -> Self::Owned {
        self.inner.to_owned()
    }
    fn clone_onto(&self, other: &mut Self::Owned) {
        <T as ToOwned>::clone_into(self.inner, other)
    }
    fn compare(&self, other: &Self::Owned) -> Ordering {
        use std::borrow::Borrow;
        self.inner.cmp(&other.borrow())
    }
    fn borrow_as(other: &'a Self::Owned) -> Self {
        use std::borrow::Borrow;
        Self {
            inner: other.borrow(),
        }
    }
}

impl<'a, T> std::ops::Deref for WrappedRef<'a, T> {
    type Target = T;
    fn deref(&self) -> &T {
        println!("BOO!");
        self.inner
    }
}

use differential_dataflow::trace::implementations::merge_batcher_col::ColumnatedMergeBatcher;
use differential_dataflow::trace::implementations::ord_neu::{OrdValBatch, OrdValBuilder};
use differential_dataflow::trace::implementations::spine_fueled::Spine;
use differential_dataflow::trace::implementations::Layout;
use differential_dataflow::trace::implementations::Update;
use differential_dataflow::trace::rc_blanket_impls::RcBuilder;
use std::rc::Rc;

pub type WrappedSpine<K, V, T, R> = Spine<
    Rc<OrdValBatch<Wrap<((K, V), T, R)>>>,
    ColumnatedMergeBatcher<K, V, T, R>,
    RcBuilder<OrdValBuilder<Wrap<((K, V), T, R)>>>,
>;

/// A layout based on timely stacks
pub struct Wrap<U: Update> {
    phantom: std::marker::PhantomData<U>,
}

impl<U: Update> Layout for Wrap<U>
where
    U::Key: 'static,
    U::Val: 'static,
{
    type Target = U;
    type KeyContainer = WrappedVec<U::Key>;
    type ValContainer = WrappedVec<U::Val>;
    type UpdContainer = Vec<(U::Time, U::Diff)>;
}

use mz_repr::fixed_length::IntoRowByTypes;
use mz_repr::ColumnType;
impl<'b, T: IntoRowByTypes> IntoRowByTypes for WrappedRef<'b, T> {
    type DatumIter<'a> = T::DatumIter<'a> where T: 'a, Self: 'a;
    fn into_datum_iter<'a>(&'a self, types: Option<&[ColumnType]>) -> Self::DatumIter<'a> {
        self.inner.into_datum_iter(types)
    }
}
