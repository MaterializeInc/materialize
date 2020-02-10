// Copyright 2019 The Rust Project Contributors
// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.
//
// Portions of this file are derived from the Option implementation in the
// libcore crate distributed as part of the Rust project. The original source
// code was retrieved on April 18, 2019 from:
//
//     https://github.com/rust-lang/rust/blob/e928e9441157f63a776ba1f8773818838e0912ea/src/libcore/option.rs
//
// The original source code is subject to the terms of the MIT license, a copy
// of which can be found in the LICENSE file at the root of this repository.

//! Option utilities.

use std::ops::Deref;

/// Extension methods for [`std::option::Option`].
pub trait OptionExt<T> {
    /// Converts from `Option<&T>` to `Option<T::Owned>` when `T` implements
    /// [`ToOwned`].
    ///
    /// The canonical use case is converting from an `Option<&str>` to an
    /// `Option<String>`.
    ///
    /// The name is symmetric with [`Option::cloned`].
    fn owned(&self) -> Option<<<T as Deref>::Target as ToOwned>::Owned>
    where
        T: Deref,
        T::Target: ToOwned;
}

impl<T> OptionExt<T> for Option<T> {
    fn owned(&self) -> Option<<<T as Deref>::Target as ToOwned>::Owned>
    where
        T: Deref,
        T::Target: ToOwned,
    {
        self.as_ref().map(|x| x.deref().to_owned())
    }
}
