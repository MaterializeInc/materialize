// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! `EXPLAIN` support for various intermediate representations.
//!
//! Ideally, the `EXPLAIN` support for each IR should be in the crate where this
//! IR is defined. However, we need to resort to an [`Explainable`] newtype
//! struct in order to provide alternate [`mz_repr::explain::Explain`]
//! implementations for some structs (see the [`mir`]) module for details.

pub(crate) mod fast_path;
pub(crate) mod hir;
pub(crate) mod lir;
pub(crate) mod mir;
pub(crate) mod optimizer_trace;

/// Newtype struct for wrapping types that should
/// implement the [`mz_repr::explain::Explain`] trait.
pub(crate) struct Explainable<'a, T>(&'a mut T);

impl<'a, T> Explainable<'a, T> {
    pub(crate) fn new(t: &'a mut T) -> Explainable<'a, T> {
        Explainable(t)
    }
}
