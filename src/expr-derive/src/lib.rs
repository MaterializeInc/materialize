// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Proc macros to derive SQL function traits.

/// Derive function traits for SQL functions.
#[proc_macro_attribute]
pub fn sqlfunc(
    attr: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    mz_expr_derive_impl::sqlfunc(attr.into(), item.into(), true)
        .unwrap_or_else(|err| err.write_errors())
        .into()
}
