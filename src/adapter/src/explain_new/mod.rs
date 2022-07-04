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
//! Ideally, the `EXPLAIN` support for each IR should be in the
//! crate where this IR is defined. However, due to the use of
//! some generic structs with fields specific to LIR in the current
//! explain paths, and the dependency chain between the crates where
//! the various IRs live, this is not possible. Consequencly, we
//! currently resort to using a wrapper type.

use mz_expr::{ExprHumanizer, RowSetFinishing};

pub(crate) mod hir;

/// Newtype struct for wrapping types that should
/// implement the `Explain` trait.
pub(crate) struct Explainable<'a, T>(&'a mut T);

impl<'a, T> Explainable<'a, T> {
    pub(crate) fn new(t: &'a mut T) -> Explainable<'a, T> {
        Explainable(t)
    }
}

/// Explain context shared by all `Explain` implementations
/// in this crate.
#[derive(Debug)]
pub struct ExplainContext<'a> {
    pub humanizer: &'a dyn ExprHumanizer,
    pub finishing: Option<RowSetFinishing>,
}
