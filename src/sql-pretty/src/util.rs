// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Utility functions used by the doc functions.

use pretty::{Doc, RcDoc};

use crate::TAB;

pub(crate) fn nest<'a>(title: RcDoc<'a>, v: RcDoc<'a>) -> RcDoc<'a> {
    RcDoc::intersperse([title, v], Doc::line())
        .nest(TAB)
        .group()
}

pub(crate) fn nest_title<S>(title: S, v: RcDoc) -> RcDoc
where
    S: Into<String>,
{
    nest(RcDoc::text(title.into()), v)
}

pub(crate) fn title_comma_separate<'a, F, T, S>(title: S, f: F, v: &'a [T]) -> RcDoc<'a, ()>
where
    F: Fn(&'a T) -> RcDoc<'a, ()>,
    S: Into<String>,
{
    let title = RcDoc::text(title.into());
    if v.is_empty() {
        title
    } else {
        nest(title, comma_separate(f, v))
    }
}

pub(crate) fn comma_separate<'a, F, T>(f: F, v: &'a [T]) -> RcDoc<'a, ()>
where
    F: Fn(&'a T) -> RcDoc<'a, ()>,
{
    let docs = v.iter().map(f).collect();
    comma_separated(docs)
}

pub(crate) fn comma_separated(v: Vec<RcDoc>) -> RcDoc {
    RcDoc::intersperse(v, RcDoc::concat([RcDoc::text(","), RcDoc::line()])).group()
}

pub(crate) fn bracket<A: Into<String>, B: Into<String>>(left: A, d: RcDoc, right: B) -> RcDoc {
    bracket_doc(
        RcDoc::text(left.into()),
        d,
        RcDoc::text(right.into()),
        RcDoc::line_(),
    )
}

pub(crate) fn bracket_doc<'a>(
    left: RcDoc<'a>,
    d: RcDoc<'a>,
    right: RcDoc<'a>,
    line: RcDoc<'a>,
) -> RcDoc<'a> {
    RcDoc::concat([
        left,
        RcDoc::concat([line.clone(), d]).nest(TAB),
        line,
        right,
    ])
    .group()
}
