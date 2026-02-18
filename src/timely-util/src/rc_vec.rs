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

//! A newtype wrapper around `Rc<C>` that implements differential-dataflow container traits.
//!
//! Differential-dataflow's container traits ([`Negate`], [`Enter`], [`Leave`], [`ResultsIn`])
//! are implemented for `Vec` but not for `Rc<Vec>`. Upstream PR #666 adds these impls for `Rc<C>`
//! but isn't released yet, and orphan rules prevent us from implementing foreign traits on `Rc<C>`
//! locally. This newtype wrapper provides the needed impls.

use std::fmt::Debug;
use std::ops::Deref;
use std::rc::Rc;

use differential_dataflow::collection::containers::{Enter, Leave, Negate, ResultsIn};
use timely::container::{Accountable, DrainContainer};

/// A newtype around [`Rc<C>`] that implements differential-dataflow container traits.
///
/// `RcVec` provides the same cheap-clone semantics as `Rc<C>` while also implementing
/// the [`Negate`], [`Enter`], [`Leave`], and [`ResultsIn`] traits required by
/// differential-dataflow's `Collection` methods.
///
/// The trait implementations use `make_mut` to obtain owned access to the inner container,
/// which is zero-cost when the `Rc` refcount is 1 (the common case in point-to-point dataflow
/// channels).
#[derive(Clone, Default, Debug)]
pub struct RcVec<C>(pub Rc<C>);

impl<C> Deref for RcVec<C> {
    type Target = C;
    #[inline]
    fn deref(&self) -> &C {
        &self.0
    }
}

impl<C: Clone> RcVec<C> {
    /// Returns a mutable reference to the inner container, cloning if necessary.
    ///
    /// This mirrors [`Rc::make_mut`] and is zero-cost when refcount is 1.
    #[inline]
    pub fn make_mut(this: &mut Self) -> &mut C {
        Rc::make_mut(&mut this.0)
    }
}

impl<C> From<C> for RcVec<C> {
    #[inline]
    fn from(inner: C) -> Self {
        RcVec(Rc::new(inner))
    }
}

// --- timely container traits ---

impl<C: Accountable> Accountable for RcVec<C> {
    #[inline]
    fn record_count(&self) -> i64 {
        self.0.record_count()
    }
    #[inline]
    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl<C> DrainContainer for RcVec<C>
where
    for<'a> &'a C: IntoIterator,
{
    type Item<'a>
        = <&'a C as IntoIterator>::Item
    where
        Self: 'a;
    type DrainIter<'a>
        = <&'a C as IntoIterator>::IntoIter
    where
        Self: 'a;
    #[inline]
    fn drain(&mut self) -> Self::DrainIter<'_> {
        self.0.as_ref().into_iter()
    }
}

// --- differential-dataflow container traits ---

impl<C: Negate + Default + Clone> Negate for RcVec<C> {
    fn negate(mut self) -> Self {
        std::mem::take(Self::make_mut(&mut self)).negate().into()
    }
}

impl<T1, T2, C> Enter<T1, T2> for RcVec<C>
where
    C: Enter<T1, T2> + Default + Clone,
{
    type InnerContainer = RcVec<C::InnerContainer>;
    fn enter(mut self) -> Self::InnerContainer {
        RcVec(Rc::new(
            std::mem::take(Self::make_mut(&mut self)).enter(),
        ))
    }
}

impl<T1, T2, C> Leave<T1, T2> for RcVec<C>
where
    C: Leave<T1, T2> + Default + Clone,
{
    type OuterContainer = RcVec<C::OuterContainer>;
    fn leave(mut self) -> Self::OuterContainer {
        RcVec(Rc::new(
            std::mem::take(Self::make_mut(&mut self)).leave(),
        ))
    }
}

impl<TS, C: ResultsIn<TS> + Default + Clone> ResultsIn<TS> for RcVec<C> {
    fn results_in(mut self, step: &TS) -> Self {
        std::mem::take(Self::make_mut(&mut self))
            .results_in(step)
            .into()
    }
}
