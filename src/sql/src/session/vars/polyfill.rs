// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The types in this module exist to overcome limitations of the Rust compiler, namely limitations
//! in const generics.

use super::Value;

/// Trait that helps us convert a `fn() -> &'static impl Value` to a `fn() -> &'static dyn Value`.
///
/// For creating a type that implements [`LazyValueFn`] see the [`lazy_value`] macro.
///
/// Note: Ideally we could use const generics to pass a function pointer directly to
/// [`VarDefinition::new`], but Rust doesn't currently support this.
///
/// [`VarDefinition::new`]: crate::session::vars::VarDefinition::new
pub trait LazyValueFn<V: Value>: Copy {
    const LAZY_VALUE_FN: fn() -> &'static dyn Value = || {
        let dyn_val: &'static dyn Value = Self::generate_value();
        dyn_val
    };

    fn generate_value() -> &'static V;
}

/// Creates a temporary struct that implements [`LazyValueFn`] and returns an instance of it.
///
///
macro_rules! lazy_value {
    ($t: ty; $f: expr) => {{
        // Note: We derive `Copy` on this type to avoid issues with `Drop` in const contexts.
        #[derive(Copy, Clone)]
        struct LazyFn(());

        impl crate::session::vars::polyfill::LazyValueFn<$t> for LazyFn {
            fn generate_value() -> &'static $t {
                static MY_VALUE_LAZY: ::std::sync::LazyLock<$t> = LazyLock::new($f);
                &*MY_VALUE_LAZY
            }
        }

        LazyFn(())
    }};
}
pub(crate) use lazy_value;

/// Assigns the provided value to a `static`, and returns a reference to it.
///
/// Note: [`VarDefinition::new`] requires a `&'static V`, where `V: Value`. Ideally we would be
/// able to pass the value as a const generic parameter, but Rust doesn't yet support this.
///
/// [`VarDefinition::new`]: crate::session::vars::VarDefinition::new
macro_rules! value {
    ($t: ty; $l: expr) => {{
        static MY_STATIC: $t = $l;
        &MY_STATIC
    }};
}
pub(crate) use value;
