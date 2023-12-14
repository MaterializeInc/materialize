// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Structs and traits for `EXPLAIN AS TEXT`.

use std::fmt;

use mz_ore::str::Indent;

use crate::explain::{
    CompactScalarSeq, CompactScalars, ExprHumanizer, IndexUsageType, Indices, ScalarOps,
    UnsupportedFormat, UsedIndexes,
};

/// A trait implemented by explanation types that can be rendered as
/// [`super::ExplainFormat::Text`].
pub trait DisplayText<C = ()>
where
    Self: Sized,
{
    fn fmt_text(&self, f: &mut fmt::Formatter<'_>, ctx: &mut C) -> fmt::Result;
}

impl<T> DisplayText for &T
where
    T: DisplayText,
{
    fn fmt_text(&self, f: &mut fmt::Formatter<'_>, ctx: &mut ()) -> fmt::Result {
        (*self).fmt_text(f, ctx)
    }
}

impl<A, C> DisplayText<C> for Box<A>
where
    A: DisplayText<C>,
{
    fn fmt_text(&self, f: &mut fmt::Formatter<'_>, ctx: &mut C) -> fmt::Result {
        self.as_ref().fmt_text(f, ctx)
    }
}

impl<A, C> DisplayText<C> for Option<A>
where
    A: DisplayText<C>,
{
    fn fmt_text(&self, f: &mut fmt::Formatter<'_>, ctx: &mut C) -> fmt::Result {
        if let Some(val) = self {
            val.fmt_text(f, ctx)
        } else {
            fmt::Result::Ok(())
        }
    }
}

impl DisplayText for UnsupportedFormat {
    fn fmt_text(&self, _f: &mut fmt::Formatter<'_>, _ctx: &mut ()) -> fmt::Result {
        unreachable!()
    }
}

impl<'a, C> DisplayText<C> for UsedIndexes
where
    C: AsMut<Indent> + AsRef<&'a dyn ExprHumanizer>,
{
    fn fmt_text(&self, f: &mut fmt::Formatter<'_>, ctx: &mut C) -> fmt::Result {
        writeln!(f, "{}Used Indexes:", ctx.as_mut())?;
        *ctx.as_mut() += 1;
        for (id, usage_types) in &self.0 {
            let usage_types = IndexUsageType::display_vec(usage_types);
            if let Some(name) = ctx.as_ref().humanize_id(*id) {
                writeln!(f, "{}- {} ({})", ctx.as_mut(), name, usage_types)?;
            } else {
                writeln!(f, "{}- [DELETED INDEX] ({})", ctx.as_mut(), usage_types)?;
            }
        }
        *ctx.as_mut() -= 1;
        Ok(())
    }
}

impl<'a> fmt::Display for Indices<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut is_first = true;
        let mut slice = self.0;
        while !slice.is_empty() {
            if !is_first {
                write!(f, ", ")?;
            }
            is_first = false;
            let lead = &slice[0];
            if slice.len() > 2 && slice[1] == lead + 1 && slice[2] == lead + 2 {
                let mut last = 3;
                while slice.get(last) == Some(&(lead + last)) {
                    last += 1;
                }
                write!(f, "#{}..=#{}", lead, lead + last - 1)?;
                slice = &slice[last..];
            } else {
                write!(f, "#{}", slice[0])?;
                slice = &slice[1..];
            }
        }
        Ok(())
    }
}

impl<'a, T> std::fmt::Display for CompactScalarSeq<'a, T>
where
    T: ScalarOps + fmt::Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let mut is_first = true;
        let mut slice = self.0;
        while !slice.is_empty() {
            if !is_first {
                write!(f, ", ")?;
            }
            is_first = false;
            if let Some(lead) = slice[0].match_col_ref() {
                if slice.len() > 2 && slice[1].references(lead + 1) && slice[2].references(lead + 2)
                {
                    let mut last = 3;
                    while slice
                        .get(last)
                        .map(|expr| expr.references(lead + last))
                        .unwrap_or(false)
                    {
                        last += 1;
                    }
                    slice[0].fmt(f)?;
                    write!(f, "..=")?;
                    slice[last - 1].fmt(f)?;
                    slice = &slice[last..];
                } else {
                    slice[0].fmt(f)?;
                    slice = &slice[1..];
                }
            } else {
                slice[0].fmt(f)?;
                slice = &slice[1..];
            }
        }
        Ok(())
    }
}

impl<T, I> fmt::Display for CompactScalars<T, I>
where
    T: ScalarOps + fmt::Display,
    I: Iterator<Item = T> + Clone,
{
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        enum State<T> {
            Start,
            FoundOne(T, usize),    // (x, x_col)
            FoundTwo(T, T, usize), // (x, y, y_col)
            FoundRun(T, T, usize), // (x, y, y_col)
        }

        let mut state = State::Start;

        let mut is_first = true;
        let mut prefix = || {
            if std::mem::replace(&mut is_first, false) {
                ""
            } else {
                ", "
            }
        };

        for n in self.0.clone() {
            state = match state {
                State::Start => match n.match_col_ref() {
                    Some(n_col) => {
                        State::FoundOne(n, n_col) // Next state
                    }
                    None => {
                        write!(f, "{}{n}", prefix())?;
                        State::Start // New match
                    }
                },
                State::FoundOne(x, x_col) => match n.match_col_ref() {
                    Some(n_col) => {
                        if x_col + 1 == n_col {
                            State::FoundTwo(x, n, n_col) // Next state
                        } else {
                            write!(f, "{}{x}", prefix())?;
                            State::FoundOne(n, n_col) // Reset match
                        }
                    }
                    None => {
                        write!(f, "{}{x}, {n}", prefix())?;
                        State::Start // New match
                    }
                },
                State::FoundTwo(x, y, y_col) => match n.match_col_ref() {
                    Some(n_col) => {
                        if y_col + 1 == n_col {
                            State::FoundRun(x, n, n_col) // Next state
                        } else {
                            write!(f, "{}{x}, {y}", prefix())?;
                            State::FoundOne(n, n_col) // Reset match
                        }
                    }
                    None => {
                        write!(f, "{}{x}, {y}, {n}", prefix())?;
                        State::Start // New match
                    }
                },
                State::FoundRun(x, y, y_col) => match n.match_col_ref() {
                    Some(n_col) => {
                        if y_col + 1 == n_col {
                            State::FoundRun(x, n, n_col) // Extend run
                        } else {
                            write!(f, "{}{x}..={y}", prefix())?;
                            State::FoundOne(n, n_col) // Reset match
                        }
                    }
                    None => {
                        write!(f, "{}{x}..={y}, {n}", prefix())?;
                        State::Start // Reset state
                    }
                },
            };
        }

        match state {
            State::Start => {
                // Do nothing
            }
            State::FoundOne(x, _) => {
                write!(f, "{}{x}", prefix())?;
            }
            State::FoundTwo(x, y, _) => {
                write!(f, "{}{x}, {y}", prefix())?;
            }
            State::FoundRun(x, y, _) => {
                write!(f, "{}{x}..={y}", prefix())?;
            }
        }

        Ok(())
    }
}

/// Render a type `t: T` as [`super::ExplainFormat::Text`].
///
/// # Panics
///
/// Panics if the [`DisplayText::fmt_text`] call returns a [`fmt::Error`].
pub fn text_string<T: DisplayText>(t: &T) -> String {
    struct TextString<'a, T>(&'a T);

    impl<'a, F: DisplayText> fmt::Display for TextString<'a, F> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            self.0.fmt_text(f, &mut ())
        }
    }

    TextString::<'_>(t).to_string()
}

/// Apply `f: F` to create a rendering context of type `C` and render the given
/// tree `t: T` within that context.
/// # Panics
///
/// Panics if the [`DisplayText::fmt_text`] call returns a [`fmt::Error`].
pub fn text_string_at<'a, T: DisplayText<C>, C, F: Fn() -> C>(t: &'a T, f: F) -> String {
    struct TextStringAt<'a, T, C, F: Fn() -> C> {
        t: &'a T,
        f: F,
    }

    impl<T: DisplayText<C>, C, F: Fn() -> C> DisplayText<()> for TextStringAt<'_, T, C, F> {
        fn fmt_text(&self, f: &mut fmt::Formatter<'_>, _ctx: &mut ()) -> fmt::Result {
            let mut ctx = (self.f)();
            self.t.fmt_text(f, &mut ctx)
        }
    }

    text_string(&TextStringAt { t, f })
}
