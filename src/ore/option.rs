// Copyright 2019 The Rust Project Contributors
// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.
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
pub trait OptionExt<T: Deref> {
    /// Converts from `Option<T>` (or `&Option<T>`) to `Option<&T::Target>`.
    ///
    /// Leaves the original `Option` in-place, creating a new one containing a
    /// reference to the inner type's `Deref::Target` type.
    ///
    /// This method is awaiting stabilization upstream ([#50264]).
    ///
    /// [#50264]: https://github.com/rust-lang/rust/issues/50264
    fn as_deref(&self) -> Option<&T::Target>;
}

impl<T: Deref> OptionExt<T> for Option<T> {
    fn as_deref(&self) -> Option<&T::Target> {
        self.as_ref().map(Deref::deref)
    }
}
