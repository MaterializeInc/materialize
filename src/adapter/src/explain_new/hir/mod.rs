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

use mz_ore::id_gen::IdGen;
use mz_repr::explain_new::{Explain, ExplainConfig, ExplainError, UnsupportedFormat};
use mz_sql::plan::HirRelationExpr;
use std::collections::{BTreeMap, HashMap};
use text::HirRelationExprExplanation;

use super::{ExplainContext, Explainable};

impl<'a> Explain<'a> for Explainable<'a, HirRelationExpr> {
    type Context = ExplainContext<'a>;

    type Text = HirRelationExprExplanation<'a>;

    type Json = UnsupportedFormat;

    type Dot = UnsupportedFormat;

    fn explain_text(
        &'a mut self,
        config: &'a ExplainConfig,
        context: &'a Self::Context,
    ) -> Result<Self::Text, ExplainError> {
        let mut explanation = HirRelationExprExplanation::new(
            self.0,
            context.humanizer,
            &mut IdGen::default(),
            HashMap::new(),
        );
        if let Some(finishing) = context.finishing.clone() {
            explanation.explain_row_set_finishing(finishing);
        }
        if config.types {
            explanation.explain_types(&BTreeMap::new());
        }
        Ok(explanation)
    }
}
