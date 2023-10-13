// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Structs and traits for `EXPLAIN AS DOT`.

use crate::explain::*;

/// A trait implemented by explanation types that can be rendered as
/// [`ExplainFormat::Dot`].
pub trait DisplayDot<C = ()>
where
    Self: Sized,
{
    fn fmt_dot(&self, f: &mut fmt::Formatter<'_>, ctx: &mut C) -> fmt::Result;
}

impl<A, C> DisplayDot<C> for Box<A>
where
    A: DisplayDot<C>,
{
    fn fmt_dot(&self, f: &mut fmt::Formatter<'_>, ctx: &mut C) -> fmt::Result {
        self.as_ref().fmt_dot(f, ctx)
    }
}

impl<A, C> DisplayDot<C> for Option<A>
where
    A: DisplayDot<C>,
{
    fn fmt_dot(&self, f: &mut fmt::Formatter<'_>, ctx: &mut C) -> fmt::Result {
        if let Some(val) = self {
            val.fmt_dot(f, ctx)
        } else {
            fmt::Result::Ok(())
        }
    }
}

impl DisplayDot for UnsupportedFormat {
    fn fmt_dot(&self, _f: &mut fmt::Formatter<'_>, _ctx: &mut ()) -> fmt::Result {
        unreachable!()
    }
}

/// Render a type `t: T` as [`ExplainFormat::Dot`].
///
/// # Panics
///
/// Panics if the [`DisplayDot::fmt_dot`] call returns a [`fmt::Error`].
pub fn dot_string<T: DisplayDot<()>>(t: &T) -> String {
    struct DotString<'a, T>(&'a T);

    impl<'a, T: DisplayDot> fmt::Display for DotString<'a, T> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            self.0.fmt_dot(f, &mut ())
        }
    }

    DotString::<'_>(t).to_string()
}

/// Apply `f: F` to create a rendering context of type `C` and render the given
/// type `t: T` as [`ExplainFormat::Dot`] within that context.
///
/// # Panics
///
/// Panics if the [`DisplayDot::fmt_dot`] call returns a [`fmt::Error`].
pub fn dot_string_at<'a, T: DisplayDot<C>, C, F: Fn() -> C>(t: &'a T, f: F) -> String {
    struct DotStringAt<'a, T, C, F: Fn() -> C> {
        t: &'a T,
        f: F,
    }

    impl<T: DisplayDot<C>, C, F: Fn() -> C> DisplayDot<()> for DotStringAt<'_, T, C, F> {
        fn fmt_dot(&self, f: &mut fmt::Formatter<'_>, _ctx: &mut ()) -> fmt::Result {
            let mut ctx = (self.f)();
            self.t.fmt_dot(f, &mut ctx)
        }
    }

    dot_string(&DotStringAt { t, f })
}
