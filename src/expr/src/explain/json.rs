// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! `EXPLAIN AS JSON` support for structures defined in this crate.

use crate::explain::{ExplainSource, PushdownInfo};
use crate::interpret::Pushdownable;
use mz_repr::explain::json::DisplayJson;

use super::{ExplainMultiPlan, ExplainSinglePlan};

impl<'a, T: 'a> DisplayJson for ExplainSinglePlan<'a, T>
where
    T: serde::Serialize,
{
    fn to_serde_value(&self) -> serde_json::Result<serde_json::Value> {
        serde_json::to_value(self.plan.plan)
    }
}

impl<'a, T: 'a> DisplayJson for ExplainMultiPlan<'a, T>
where
    T: serde::Serialize,
{
    fn to_serde_value(&self) -> serde_json::Result<serde_json::Value> {
        let plans = self
            .plans
            .iter()
            .map(|(id, plan)| {
                // TODO: fix plans with Constants
                serde_json::json!({
                    "id": id,
                    "plan": &plan.plan
                })
            })
            .collect::<Vec<_>>();

        let sources = self
            .sources
            .iter()
            .map(
                |ExplainSource {
                     id,
                     op,
                     pushdown_info,
                 }| {
                    let mut json = serde_json::json!({
                        "id": id,
                        "op": op,
                    });

                    if let Some(PushdownInfo { trace }) = pushdown_info {
                        let pushdownable: Vec<_> = trace
                            .iter()
                            .enumerate()
                            .filter(|(_, p)| **p == Pushdownable::Yes)
                            .map(|(i, _)| i)
                            .collect();

                        json.as_object_mut()
                            .unwrap()
                            .insert("pushdown".to_owned(), serde_json::json!(pushdownable));
                    }

                    json
                },
            )
            .collect::<Vec<_>>();

        let result = serde_json::json!({ "plans": plans, "sources": sources });

        Ok(result)
    }
}
