// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Regions that do not create a timely subgraph.
//!
//! A timely region is a full subgraph with its own progress tracker and progress exchange, and
//! the scheduler visits it several times per tick. Rendering uses regions only to group
//! operators, so these helpers run the region body in the enclosing scope and make entering
//! and leaving the identity. Operators are created exactly as before, only the region scopes
//! are gone.

use differential_dataflow::Collection;
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::trace::TraceReader;
use timely::dataflow::Scope;
use timely::progress::Timestamp;

/// Region creation on a scope that does not create a subgraph.
pub(crate) trait InlineRegionScope<'a, T: Timestamp> {
    /// Runs `func` in this scope. The name is ignored.
    fn inline_named<R>(&self, name: &str, func: impl FnOnce(Scope<'a, T>) -> R) -> R;
}

impl<'a, T: Timestamp> InlineRegionScope<'a, T> for Scope<'a, T> {
    fn inline_named<R>(&self, _name: &str, func: impl FnOnce(Scope<'a, T>) -> R) -> R {
        func(*self)
    }
}

/// Entering and leaving a flat region, which is the identity.
pub(crate) trait InlineRegion<'a, T: Timestamp>: Sized {
    /// Brings `self` into the region `scope`, which is the scope it already lives in.
    fn enter_inline(self, _scope: Scope<'a, T>) -> Self {
        self
    }

    /// Returns `self` to the scope `outer`, which is the scope it already lives in.
    fn leave_inline(self, _outer: Scope<'a, T>) -> Self {
        self
    }
}

impl<'a, T: Timestamp, C> InlineRegion<'a, T> for Collection<'a, T, C> {}

impl<'a, Tr: TraceReader> InlineRegion<'a, Tr::Time> for Arranged<'a, Tr> {}
