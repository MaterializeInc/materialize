// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Structs and traits for `EXPLAIN AS JSON`.

use mz_sql_parser::ast::display::AstDisplay;
use mz_sql_parser::ast::{Query, Raw};

use crate::explain::*;

/// A trait implemented by explanation types that can be rendered as
/// [`ExplainFormat::Syntax`].
pub trait DisplaySyntax<C = ()>
where
    Self: Sized,
{
    fn to_sql_query(&self, ctx: &mut C) -> Query<Raw>;
}

/// Render a type `t: T` as [`ExplainFormat::Syntax`].
pub fn syntax_string<T: DisplaySyntax>(t: &T) -> String {
    let query = t.to_sql_query(&mut ());
    query.to_ast_string()
}

impl DisplaySyntax for UnsupportedFormat {
    fn to_sql_query(&self, _ctx: &mut ()) -> Query<Raw> {
        unreachable!()
    }
}
