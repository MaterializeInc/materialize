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

use mz_ore::str::{Indent, IndentLike};

use crate::explain::{
    CompactScalarSeq, ExprHumanizer, Indices, ScalarOps, UnsupportedFormat, UsedIndexes,
};
use crate::Row;

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
        for id in &self.0 {
            let index_name = ctx
                .as_ref()
                .humanize_id(*id)
                .unwrap_or_else(|| id.to_string());
            writeln!(f, "{}- {}", ctx.as_mut(), index_name)?;
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

fn write_first_rows(
    f: &mut fmt::Formatter<'_>,
    first_rows: &Vec<(&Row, &crate::Diff)>,
    ctx: &mut Indent,
) -> fmt::Result {
    for (row, diff) in first_rows {
        if **diff == 1 {
            writeln!(f, "{}- {}", ctx, row)?;
        } else {
            writeln!(f, "{}- ({} x {})", ctx, row, diff)?;
        }
    }
    Ok(())
}

pub fn fmt_text_constant_rows<'a, I>(
    f: &mut fmt::Formatter<'_>,
    mut rows: I,
    ctx: &mut Indent,
) -> fmt::Result
where
    I: Iterator<Item = (&'a Row, &'a crate::Diff)>,
{
    let mut row_count = 0;
    let mut first_rows = Vec::with_capacity(20);
    for _ in 0..20 {
        if let Some((row, diff)) = rows.next() {
            row_count += diff.abs();
            first_rows.push((row, diff));
        }
    }
    let rest_of_row_count = rows.map(|(_, diff)| diff.abs()).sum::<crate::Diff>();
    if rest_of_row_count != 0 {
        writeln!(
            f,
            "{}total_rows (diffs absed): {}",
            ctx,
            row_count + rest_of_row_count
        )?;
        writeln!(f, "{}first_rows:", ctx)?;
        ctx.indented(move |ctx| write_first_rows(f, &first_rows, ctx))?;
    } else {
        write_first_rows(f, &first_rows, ctx)?;
    }
    Ok(())
}
