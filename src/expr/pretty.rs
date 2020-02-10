// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// Portions of this file are derived from the `chain!` macro in the Gluon
// project. The original source code was retrieved on August 28, 2019 from:
//
//     https://github.com/gluon-lang/gluon/blob/259cedfea829f99b87d3f0e23ca43175216106d1/base/src/lib.rs#L68-L76
//
// The original source code is subject to the terms of the MIT license, a copy
// of which can be found in the LICENSE file at the root of this repository.

//! Making [`pretty`] Pretty Again

use std::collections::VecDeque;

use pretty::{BuildDoc, DocAllocator, DocBuilder};

use ore::collections::CollectionExt;

pub trait DocBuilderExt {
    fn embrace(self, left: &'static str, right: &'static str) -> Self;

    fn tightly_embrace(self, left: &'static str, right: &'static str) -> Self;
}

impl<'a, D> DocBuilderExt for DocBuilder<'a, D>
where
    D: DocAllocator<'a>,
{
    /// Embrace `doc` as the sequence `left`, `doc`, `right`. The resulting document
    /// has one of two layouts:
    ///
    ///   * It has a space after `left` and a space before `right`, if it fits onto
    ///     a line;
    ///   * It has a newline after `left` and a newline before `right` while
    ///     indenting `doc` by 2 spaces, otherwise.
    ///
    /// For example:
    /// ```text
    /// to_braced_doc("Distinct {", "665", "}")
    /// ```
    /// results in
    /// ```text
    /// Distinct { 665 }
    /// ```
    /// when fitting on a single line and in
    /// ```text
    /// Distinct {
    ///   665
    /// }
    /// ```
    /// otherwise.
    fn embrace(self, left: &'static str, right: &'static str) -> Self {
        let alloc = self.0;
        alloc
            .text(left)
            .append(alloc.line().append(self).nest(2))
            .append(alloc.line())
            .append(right)
            .group()
    }

    /// Tightly embrace `doc` as the sequence `left`, `doc`, `right`. The resulting
    /// document has one of two layouts:
    ///
    ///   * It concatenates `left`, `doc`, and `right` without spacing, if it fits
    ///     onto a line;
    ///   * It has a newline after `left` and a newline before `right` while
    ///     indenting `doc` by 2 spaces, otherwise.
    ///
    /// For example:
    /// ```text
    /// to_tightly_braced_doc("[", "v665", "]")
    /// ```
    /// results in
    /// ```text
    /// [v665]
    /// ```
    /// when fitting on a single line and in
    /// ```text
    /// [
    ///   v665
    /// ]
    /// ```
    /// otherwise.
    fn tightly_embrace(self, left: &'static str, right: &'static str) -> Self {
        let alloc = self.0;
        alloc
            .text(left)
            .append(alloc.line_().append(self).nest(2))
            .append(alloc.line_())
            .append(right)
            .group()
    }
}

pub trait DocAllocatorExt<'a>: DocAllocator<'a> {
    /// Like [`DocBuilder::intersperse`], but with additional breakpoints to
    /// allow for a more compressed expanded representation.
    ///
    /// For example, a list of numbers interspersed with commas would normally
    /// be rendered as `1, 2, 3, 4, 5`, if it fits on one line, or otherwise one
    /// number per line:
    ///
    /// ```text
    /// 1,
    /// 2,
    /// 3,
    /// 4,
    /// 5
    /// ```
    ///
    /// If `compact_intersperse_doc` is used instead of [`Doc::intersperse`],
    /// the expanded representation can instead be laid out as
    ///
    /// ```text
    /// 1, 2
    /// 3, 4
    /// 5
    /// ```
    ///
    /// or:
    ///
    /// ```text
    /// 1, 2, 3, 4
    /// 5
    /// ```
    ///
    /// This isn't perfect, but it does a decent job, and works especially well
    /// when each element in `docs` has the same width. The problem space is
    /// more fully mapped out by Jean-Philippe Bernardy in ["Towards the
    /// Prettiest Printer"][blog], but his approach requires a new
    /// pretty-printing algorithm, while this approach works with the standard
    /// Wadler algorithm.
    ///
    /// [blog]: https://jyp.github.io/posts/towards-the-prettiest-printer.html
    fn compact_intersperse<I, S>(&'a self, docs: I, separator: S) -> DocBuilder<'a, Self>
    where
        I: IntoIterator,
        I::Item: Into<BuildDoc<'a, Self::Doc, ()>>,
        S: Into<BuildDoc<'a, Self::Doc, ()>> + Clone;
}

impl<'a, A> DocAllocatorExt<'a> for A
where
    A: DocAllocator<'a>,
{
    fn compact_intersperse<I, S>(&'a self, docs: I, separator: S) -> DocBuilder<'a, Self>
    where
        I: IntoIterator,
        I::Item: Into<BuildDoc<'a, Self::Doc, ()>>,
        S: Into<BuildDoc<'a, Self::Doc, ()>> + Clone,
    {
        let mut docs: VecDeque<_> = docs
            .into_iter()
            .map(|d| DocBuilder(self, d.into()))
            .collect();

        if docs.is_empty() {
            return self.nil();
        }

        // Build a binary tree of groupings. Given docs of A, B, C, D, E and
        // separator SEP, we'll build:
        //
        //     (
        //         (A SEP B).group()
        //         SEP
        //         (C SEP D).group()
        //     ).group()
        //     SEP
        //     E.group
        //
        while docs.len() != 1 {
            let chunks = docs.len() / 2;
            let odd = docs.len() % 2 == 1;
            for _ in 0..chunks {
                let left = docs.pop_front().unwrap();
                let right = docs.pop_front().unwrap();
                let merged = left.append(separator.clone()).append(right).group();
                docs.push_back(merged);
            }
            if odd {
                let last = docs.pop_front().unwrap().group();
                docs.push_back(last);
            }
        }

        DocBuilder(self, docs.into_element().into())
    }
}

/// Converts sequences of integers into sequences that may contain ranges.
///
/// A subsequence is converted to a range if it contains at least three
/// consecutive increasing numbers.
///
/// # Example
/// ```rust
/// use expr::pretty::tighten_outputs;
/// let vector = vec![0, 1, 2, 3, 5, 6];
/// let result = tighten_outputs(&vector[..]);
/// assert_eq!(result, vec!["0 .. 3".to_string(), "5".to_string(), "6".to_string()]);
/// ```
pub fn tighten_outputs(mut slice: &[usize]) -> Vec<String> {
    let mut result = Vec::new();
    while !slice.is_empty() {
        let lead = &slice[0];
        if slice.len() > 2 && slice[1] == lead + 1 && slice[2] == lead + 2 {
            let mut last = 3;
            while slice.get(last) == Some(&(lead + last)) {
                last += 1;
            }
            result.push(format!("{} .. {}", lead, lead + last - 1));
            slice = &slice[last..];
        } else {
            result.push(format!("{}", slice[0]));
            slice = &slice[1..];
        }
    }
    result
}
