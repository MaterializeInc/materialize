// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

// Portions of this file are derived from the `chain!` macro in the Gluon
// project. The original source code was retrieved on August 28, 2019 from:
//
//     https://github.com/gluon-lang/gluon/blob/259cedfea829f99b87d3f0e23ca43175216106d1/base/src/lib.rs#L68-L76
//
// The original source code is subject to the terms of the MIT license, a copy
// of which can be found in the LICENSE file at the root of this repository.

//! Making [`pretty`] Pretty Again

use crate::RelationExpr;
use ore::collections::CollectionExt;
use pretty::Doc::Space;
use pretty::{BoxDoc, Doc};
use std::collections::VecDeque;

/// Convert the arguments into a [`pretty::Doc`], that is, a *document* for
/// subsequent layout during pretty-printing. This macro makes it
/// possible to convert a variable number of arguments that also have
/// different types. Its body simply starts out with an empty document
/// [`pretty::Doc::nil`] and adds each argument via [`pretty::Doc::append`].
/// While the latter is declared to accept any argument implementing
/// `Into<Doc<'a, BoxDoc<'a, A>, A>`, in practice it only accepts instances
/// of [`pretty::Doc`] (natch), strings via [`pretty::Doc::text`], and relation
/// expressions via [`RelationExpr::to_doc`] and a specialization of the
/// `Into` trait. Sadly, `usize` is a foreign  type and `Into` is a foreign
/// trait, so converting indices inside [`RelationExpr`] into documents is a
/// tad more involved.
#[macro_export]
macro_rules! to_doc {
    ($($rest: expr),*) => {{
        let mut doc = ::pretty::Doc::Nil;
        $(
            doc = doc.append($rest);
        )*
        doc
    }}
}

impl<'a> From<&'a Box<RelationExpr>> for Doc<'a, BoxDoc<'a, ()>, ()> {
    /// Turn [`RelationExpr`] into [`pretty::Doc`] by invoking
    /// [`RelationExpr::to_doc`]. Without this trait, `to_doc!` would
    /// be far less convenient.
    fn from(s: &'a Box<RelationExpr>) -> Doc<'a, BoxDoc<'a, ()>, ()> {
        s.to_doc()
    }
}

/// Embrace `doc` as the sequence `left`, `doc`, `right`. The resulting document has one
/// of two layouts:
///
///   * It has a space after `left` and a space before `right`, if it fits onto a line;
///   * It has a newline after `left` and a newline before `right` while indenting `doc`
///     by 2 spaces, otherwise.
///
/// For example:
/// ```ignore
/// to_braced_doc("Distinct {", "665", "}")
/// ```
/// results in
/// ```ignore
/// Distinct { 665 }
/// ```
/// when fitting on a single line and in
/// ```ignore
/// Distinct {
///   665
/// }
/// ```
/// otherwise.
pub fn to_braced_doc<'a, D1, D2, D3>(left: D1, doc: D2, right: D3) -> Doc<'a, BoxDoc<'a, ()>, ()>
where
    D1: Into<Doc<'a, BoxDoc<'a, ()>, ()>>,
    D2: Into<Doc<'a, BoxDoc<'a, ()>, ()>>,
    D3: Into<Doc<'a, BoxDoc<'a, ()>, ()>>,
{
    to_doc!(left, Space.append(doc).nest(2), Space, right)
}

/// Tightly embrace `doc` as the sequence `left`, `doc`, `right`. The resulting document
/// has one of two layouts:
///
///   * It concatenates `left`, `doc`, and `right` without spacing, if it fits onto a line;
///   * It has a newline after `left` and a newline before `right` while indenting `doc`
///     by 2 spaces, otherwise.
///
/// For example:
/// ```ignore
/// to_tightly_braced_doc("[", "v665", "]")
/// ```
/// results in
/// ```ignore
/// [v665]
/// ```
/// when fitting on a single line and in
/// ```ignore
/// [
///   v665
/// ]
/// ```
/// otherwise.
pub fn to_tightly_braced_doc<'a, D1, D2, D3>(
    left: D1,
    doc: D2,
    right: D3,
) -> Doc<'a, BoxDoc<'a, ()>, ()>
where
    D1: Into<Doc<'a, BoxDoc<'a, ()>, ()>>,
    D2: Into<Doc<'a, BoxDoc<'a, ()>, ()>>,
    D3: Into<Doc<'a, BoxDoc<'a, ()>, ()>>,
{
    to_doc!(
        left,
        Doc::space_().append(doc).nest(2),
        Doc::space_(),
        right
    )
}

/// Like [`Doc::intersperse`], but with additional breakpoints to allow for a
/// more compressed expanded representation.
///
/// For example, a list of numbers interspersed with commas would normally be
/// rendered as `1, 2, 3, 4, 5`, if it fits on one line, or otherwise one number
/// per line:
///
/// ```text
/// 1,
/// 2,
/// 3,
/// 4,
/// 5
/// ```
///
/// If `compact_intersperse_doc` is used instead of [`Doc::intersperse`], the
/// expanded representation can instead be laid out as
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
/// This isn't perfect, but it does a decent job, and works especially well when
/// each element in `docs` has the same width. The problem space is more fully
/// mapped out by Jean-Philippe Bernardy in ["Towards the Prettiest
/// Printer"][blog], but his approach requires a new pretty-printing algorithm,
/// while this approach works with the standard Wadler algorithm.
///
/// [blog]: https://jyp.github.io/posts/towards-the-prettiest-printer.html
pub fn compact_intersperse_doc<'a, I, S>(docs: I, separator: S) -> Doc<'a, BoxDoc<'a, ()>, ()>
where
    I: IntoIterator,
    I::Item: Into<Doc<'a, BoxDoc<'a, ()>, ()>>,
    S: Into<Doc<'a, BoxDoc<'a, ()>, ()>> + Clone,
{
    let mut docs: VecDeque<_> = docs.into_iter().map(Into::into).collect();

    if docs.is_empty() {
        return Doc::nil();
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

    docs.into_element()
}

/// Converts sequences of integers into sequences that may contain ranges.
///
/// A subsequence is converted to a range if it contains at least three consecutive increasing numbers.
///
/// # Example
/// ```rust
/// use expr::pretty_pretty::tighten_outputs;
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
