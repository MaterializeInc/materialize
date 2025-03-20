// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Defines a "lending iterator" for [`Row`]

use std::fmt::Debug;
use std::sync::Arc;

use crate::row::{Row, RowRef};

/// An iterator that can borrow from `self` and yield [`RowRef`]s.
///
/// This trait is a "lending iterator" for [`Row`]s, in other words, an iterator that borrows from
/// self (e.g. an underlying memory buffer) to return a [`RowRef`]. The [`std::iter::Iterator`]
/// trait does not currently support this pattern because there is no way to name the lifetime of
/// the borrow on its associated `Item` type. Generic Associated Types (GATs) would allow this but
/// so far no new trait has been introduced with this API.
///
/// There are a few open source crates that provide a trait:
///
/// * [`streaming_iterator`](https://docs.rs/streaming-iterator/latest/streaming_iterator/)
/// * [`lending-iterator`](https://docs.rs/lending-iterator/latest/lending_iterator/)
///
/// Neither have an `IntoLendingIterator` trait that is useful for our interface, nor do they work
/// well with trait objects.
pub trait RowIterator: Debug {
    /// Returns the next [`RowRef`] advancing the iterator.
    fn next(&mut self) -> Option<&RowRef>;

    /// Returns the next [`RowRef`] without advancing the iterator.
    fn peek(&mut self) -> Option<&RowRef>;

    /// The total number of [`Row`]s this iterator could ever yield.
    ///
    /// Note: it _does not_ return the number of rows _remaining_, in otherwords calling `.next()`
    /// will not change the value returned from this method.
    fn count(&self) -> usize;

    /// Returns a clone of `self` as a `Box<dyn RowIterator>`.
    fn box_clone(&self) -> Box<dyn RowIterator>;

    /// Maps the returned [`RowRef`]s from this [`RowIterator`].
    fn map<T, F>(self, f: F) -> MappedRowIterator<Self, F>
    where
        Self: Sized,
        F: FnMut(&RowRef) -> T,
    {
        MappedRowIterator {
            inner: self,
            func: f,
        }
    }
}

impl<I: RowIterator + ?Sized> RowIterator for Box<I> {
    fn next(&mut self) -> Option<&RowRef> {
        (**self).next()
    }

    fn peek(&mut self) -> Option<&RowRef> {
        (**self).peek()
    }

    fn count(&self) -> usize {
        (**self).count()
    }

    fn box_clone(&self) -> Box<dyn RowIterator> {
        (**self).box_clone()
    }
}

impl<I: RowIterator + ?Sized> RowIterator for &mut I {
    fn next(&mut self) -> Option<&RowRef> {
        (**self).next()
    }

    fn peek(&mut self) -> Option<&RowRef> {
        (**self).peek()
    }

    fn count(&self) -> usize {
        (**self).count()
    }

    fn box_clone(&self) -> Box<dyn RowIterator> {
        (**self).box_clone()
    }
}

#[derive(Debug)]
pub struct MappedRowIterator<I: RowIterator, F> {
    inner: I,
    func: F,
}

impl<T, F, I: RowIterator> Iterator for MappedRowIterator<I, F>
where
    F: FnMut(&RowRef) -> T,
{
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        let row_ref = self.inner.next()?;
        Some((self.func)(row_ref))
    }
}

/// Convert a type into a [`RowIterator`].
pub trait IntoRowIterator {
    type Iter: RowIterator;
    fn into_row_iter(self) -> Self::Iter;
}

impl<T: RowIterator> IntoRowIterator for T {
    type Iter = Self;
    fn into_row_iter(self) -> Self::Iter {
        self
    }
}

/// A [`RowIterator`] for a single [`Row`].
#[derive(Debug, Clone)]
pub struct SingleRowIter {
    row: Arc<Row>,
    finished: bool,
}

impl RowIterator for SingleRowIter {
    fn next(&mut self) -> Option<&RowRef> {
        if self.finished {
            None
        } else {
            self.finished = true;
            Some(self.row.as_ref())
        }
    }

    fn peek(&mut self) -> Option<&RowRef> {
        if self.finished {
            None
        } else {
            Some(self.row.as_ref())
        }
    }

    fn count(&self) -> usize {
        1
    }

    fn box_clone(&self) -> Box<dyn RowIterator> {
        Box::new(self.clone())
    }
}

impl IntoRowIterator for Row {
    type Iter = SingleRowIter;

    fn into_row_iter(self) -> Self::Iter {
        SingleRowIter {
            row: Arc::new(self),
            finished: false,
        }
    }
}

/// A [`RowIterator`] for a [`Vec`] of [`Row`]s.
#[derive(Debug, Clone)]
pub struct VecRowIter {
    rows: Arc<[Row]>,
    index: usize,
}

impl RowIterator for VecRowIter {
    fn next(&mut self) -> Option<&RowRef> {
        let row = self.rows.get(self.index).map(|r| r.as_ref())?;
        self.index = self.index.saturating_add(1);

        Some(row)
    }

    fn peek(&mut self) -> Option<&RowRef> {
        self.rows.get(self.index).map(|r| r.as_ref())
    }

    fn count(&self) -> usize {
        self.rows.len()
    }

    fn box_clone(&self) -> Box<dyn RowIterator> {
        Box::new(self.clone())
    }
}

impl IntoRowIterator for Vec<Row> {
    type Iter = VecRowIter;

    fn into_row_iter(self) -> Self::Iter {
        VecRowIter {
            rows: self.into(),
            index: 0,
        }
    }
}
