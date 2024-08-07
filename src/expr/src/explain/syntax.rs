// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! `EXPLAIN ... AS SYNTAX` support for structures defined in this crate.

use mz_repr::explain::syntax::DisplaySyntax;
use mz_repr::explain::PlanRenderingContext;

use crate::explain::{ExplainMultiPlan, ExplainSinglePlan};
use crate::MirRelationExpr;

impl<'a, T: 'a> DisplaySyntax for ExplainSinglePlan<'a, T>
where
    T: DisplaySyntax<PlanRenderingContext<'a, T>> + Ord,
{
    fn to_sql_query(&self, _ctx: &mut ()) -> mz_sql_parser::ast::Query<mz_sql_parser::ast::Raw> {
        todo!()
    }
}

impl<'a, T: 'a> DisplaySyntax for ExplainMultiPlan<'a, T>
where
    T: DisplaySyntax<PlanRenderingContext<'a, T>> + Ord,
{
    fn to_sql_query(&self, ctx: &mut ()) -> mz_sql_parser::ast::Query<mz_sql_parser::ast::Raw> {
        todo!()
    }
}

impl DisplaySyntax<PlanRenderingContext<'_, MirRelationExpr>> for MirRelationExpr {
    fn to_sql_query(
        &self,
        _ctx: &mut PlanRenderingContext<'_, MirRelationExpr>,
    ) -> mz_sql_parser::ast::Query<mz_sql_parser::ast::Raw> {
        todo!()
    }
}
