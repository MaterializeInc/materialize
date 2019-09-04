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
use pretty::Doc::Space;
use pretty::{BoxDoc, Doc};

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
/// ```
/// to_braced_doc("Distinct {", "665", "}")
/// ```
/// results in
/// ```
/// Distinct { 665 }
/// ```
/// when fitting on a single line and in
/// ```
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
