// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! `EXPLAIN` support for [`FastPathPlan`].

use std::collections::BTreeMap;

use mz_expr::explain::{ExplainContext, ExplainMultiPlan};
use mz_repr::explain::{AnnotatedPlan, Explain, ExplainError, UnsupportedFormat};

use crate::coord::peek::FastPathPlan;
use crate::explain::Explainable;

impl<'a> Explain<'a> for Explainable<'a, FastPathPlan> {
    type Context = ExplainContext<'a>;

    type Text = ExplainMultiPlan<'a, FastPathPlan>;

    type Json = ExplainMultiPlan<'a, FastPathPlan>;

    type Dot = UnsupportedFormat;

    fn explain_text(&'a mut self, context: &'a Self::Context) -> Result<Self::Text, ExplainError> {
        self.as_explain_multi_plan(context)
    }

    fn explain_json(&'a mut self, context: &'a Self::Context) -> Result<Self::Text, ExplainError> {
        self.as_explain_multi_plan(context)
    }
}

impl<'a> Explainable<'a, FastPathPlan> {
    fn as_explain_multi_plan(
        &'a mut self,
        context: &'a ExplainContext<'a>,
    ) -> Result<ExplainMultiPlan<'a, FastPathPlan>, ExplainError> {
        let plans = vec![(
            "Explained Query (fast path)".to_string(),
            AnnotatedPlan {
                plan: self.0,
                annotations: BTreeMap::default(),
            },
        )];

        let sources = vec![];

        Ok(ExplainMultiPlan {
            context,
            sources,
            plans,
        })
    }
}
