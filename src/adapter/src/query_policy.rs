// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Query admission against the final optimized execution plan.

use mz_compute_types::dataflows::DataflowDescription;
use mz_controller_types::ClusterId;
use mz_ore::str::StrExt;
use mz_sql::plan::{QueryPlanFeature, QueryPolicyMode};

use crate::catalog::Catalog;
use crate::coord::peek::{FastPathPlan, PeekPlan};
use crate::session::Session;
use crate::{AdapterError, AdapterNotice};

/// Features of the work actually admitted, rather than its SQL dependencies.
pub(crate) struct QueryPlanFeatures {
    temporary_dataflow: bool,
    storage_read: bool,
}

impl From<&PeekPlan> for QueryPlanFeatures {
    fn from(plan: &PeekPlan) -> Self {
        match plan {
            PeekPlan::SlowPath(dataflow) => Self::from(&dataflow.desc),
            PeekPlan::FastPath(plan) => Self {
                temporary_dataflow: false,
                storage_read: matches!(plan, FastPathPlan::PeekPersist(..)),
            },
        }
    }
}

impl<P, S, O> From<&DataflowDescription<P, S, O>> for QueryPlanFeatures {
    fn from(dataflow: &DataflowDescription<P, S, O>) -> Self {
        Self {
            // COPY TO and SUBSCRIBE also install query-scoped dataflows, even
            // when all their inputs come from existing indexes.
            temporary_dataflow: true,
            storage_read: !dataflow.source_imports.is_empty(),
        }
    }
}

/// Identifies the rule responsible for a rejection or observe-only notice.
#[derive(Clone, Debug)]
pub struct QueryPolicyViolation {
    pub policy: String,
    pub rule: String,
    pub cluster: String,
    pub feature: QueryPlanFeature,
}

impl QueryPolicyViolation {
    pub fn reason(&self) -> &'static str {
        match self.feature {
            QueryPlanFeature::TemporaryDataflow => "this query would build a temporary dataflow",
            QueryPlanFeature::StorageRead => "this query would read from object storage",
        }
    }

    pub fn detail(&self) -> String {
        format!(
            "Query policy {} rule {} applies to this query on cluster {}.",
            self.policy.quoted(),
            self.rule.quoted(),
            self.cluster.quoted(),
        )
    }

    pub fn hint(&self) -> &'static str {
        "Run EXPLAIN to inspect the query plan. Create an index that serves the query on this \
         cluster, or use a cluster whose query policy permits this plan."
    }
}

/// Check before installing a temporary dataflow or issuing a fast-path read.
///
/// The catalog snapshot is the admission boundary. Policy changes apply to
/// subsequent queries, not retroactively to work already being planned.
pub(crate) fn check_query_policies(
    catalog: &Catalog,
    session: &Session,
    cluster_id: ClusterId,
    plan: impl Into<QueryPlanFeatures>,
) -> Result<(), AdapterError> {
    if !catalog.system_config().enable_query_policy_enforcement() {
        return Ok(());
    }

    let plan = plan.into();
    let includes = |feature| match feature {
        QueryPlanFeature::TemporaryDataflow => plan.temporary_dataflow,
        QueryPlanFeature::StorageRead => plan.storage_read,
    };

    let mut rejection = None;
    let mut warned = false;
    if let Some(policy) = catalog.state().query_policy_for(cluster_id) {
        for rule in &policy.rules {
            if !includes(rule.value) {
                continue;
            }
            let violation = QueryPolicyViolation {
                policy: policy.name.clone(),
                rule: rule.name.clone(),
                cluster: catalog.get_cluster(cluster_id).name.clone(),
                feature: rule.value,
            };
            match policy.mode {
                QueryPolicyMode::Warn => {
                    warned = true;
                    session.add_notice(AdapterNotice::QueryPolicyWarning(violation));
                }
                QueryPolicyMode::Enforce => {
                    rejection.get_or_insert(violation);
                }
            }
        }
    }
    if let Some(violation) = rejection {
        session.metrics().query_policy_queries("rejected").inc();
        Err(AdapterError::QueryPolicyRejected(violation))
    } else {
        if warned {
            session.metrics().query_policy_queries("warned").inc();
        }
        Ok(())
    }
}
