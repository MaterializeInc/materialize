// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! `EXPLAIN` support for `Hir` structures.

pub(crate) mod text;

use mz_repr::explain_new::{Explain, ExplainConfig, ExplainError, UnsupportedFormat};
use mz_sql::plan::HirRelationExpr;

use super::{AnnotatedPlan, ExplainContext, ExplainSinglePlan, Explainable};

impl<'a> Explain<'a> for Explainable<'a, HirRelationExpr> {
    type Context = ExplainContext<'a>;

    type Text = ExplainSinglePlan<'a, HirRelationExpr>;

    type Json = UnsupportedFormat;

    type Dot = UnsupportedFormat;

    #[allow(unused_variables)] // TODO (#13299)
    fn explain_text(
        &'a mut self,
        config: &'a ExplainConfig,
        context: &'a Self::Context,
    ) -> Result<Self::Text, ExplainError> {
        // TODO: use config values to infer requested
        // plan annotations
        let plan = AnnotatedPlan {
            plan: self.0,
            annotations: Default::default(),
        };
        Ok(ExplainSinglePlan { context, plan })
    }
}
