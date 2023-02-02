// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Transformations that don't fit into one of the `canonicalization`,
//! `compound`, `fusion`, `movement`, or `ordering` buckets.
//!
//! Transformations in this module are meant to be revisited in the future, as
//! the current hypothesis is that a transofrmation should attempt to do two of
//! the above things (such as `fusion` and `canonicalization`) at the same time.

mod union;

pub use union::UnionNegateFusion;
