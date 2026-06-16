// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/// A copy of [`std::convert::From`] so we can work around Rust's orphan rules.
///
/// The protobuf objects we durably persist for the catalog live in the
/// [`mz_catalog_protos`] crate, because [`prost`]s heavy usage of proc-macros
/// results in very long compile times. By moving them into a separate crate we
/// need to recompile them a lot less frequently.
pub(crate) trait UpgradeFrom<T>: Sized {
    fn upgrade_from(value: T) -> Self;
}

impl<T, U> UpgradeInto<U> for T
where
    U: UpgradeFrom<T>,
{
    fn upgrade_into(self) -> U {
        U::upgrade_from(self)
    }
}

/// A copy of [`std::convert::Into`] so we can work around Rust's orphan rules.
pub(crate) trait UpgradeInto<U>: Sized {
    fn upgrade_into(self) -> U;
}
