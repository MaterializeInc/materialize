// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! Making `[pretty]` Pretty Again

use crate::RelationExpr;
use pretty::{BoxDoc, Doc};

/// Convert the arguments into a `[pretty::Doc]`, that is, a *document*
/// for subsequent layout during pretty-printing. The macro makes it
/// possible to convert a variable number of arguments that also have
/// different types. Its body starts out with the result of calling
/// `[pretty::Doc::nil]` and then calls `[pretty::Doc::append]` on each
/// argument. The latter method nominally is fully generic, but in
/// practice it uses instances of `[pretty::Doc]` as is, converts strings
/// via `[pretty::Doc::text]`, and converts relation expressions via
/// `[RelationExpr::to_doc]`—thanks to the dark magic of the `[From]`
/// and [Into]`
///
/// The macro definition was originally
/// [borrowed from Gluon](https://github.com/gluon-lang/gluon/blob/259cedfea829f99b87d3f0e23ca43175216106d1/base/src/lib.rs#L68),
/// but thereafter renamed and substantially modified. That borrowing
/// is subject to the MIT open source license and includes a requirement
/// to acknowledge Markus Westerlind with a copyright year of 2015.
#[macro_export]
macro_rules! to_doc {
    ($($rest: expr),*) => {{
        let mut doc = ::pretty::Doc::nil();
        $(
            doc = doc.append($rest);
        )*
        doc
    }}
}

/// The dark magic that makes it possible to include a relation expression
/// amongst the arguments of the `to_doc!` macro defined above.
impl<'a> From<&'a Box<RelationExpr>> for Doc<'a, BoxDoc<'a, ()>, ()> {
    fn from(s: &'a Box<RelationExpr>) -> Doc<'a, BoxDoc<'a, ()>, ()> {
        s.to_doc()
    }
}
