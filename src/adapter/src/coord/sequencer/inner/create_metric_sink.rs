// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! `CREATE METRIC SINK` sequencing.
//!
//! All this does is write the catalog item. Nothing gets optimized or shipped, so a metric sink
//! created here publishes no metrics. The item is still worth having on its own: it holds the
//! definition compute needs, and because `create_sql` is re-parsed on boot the sink survives a
//! restart.
//!
//! TODO: optimize and ship the dataflow here, and re-render it during bootstrap.

use anyhow::anyhow;
use mz_catalog::memory::error::ErrorKind;
use mz_catalog::memory::objects::{CatalogItem, MetricSink};
use mz_controller_types::ClusterId;
use mz_ore::instrument;
use mz_sql::catalog::CatalogError;
use mz_sql::names::{QualifiedItemName, ResolvedIds};
use mz_sql::plan;
use mz_sql::session::metadata::SessionMetadata;

use crate::AdapterNotice;
use crate::catalog;
use crate::command::ExecuteResponse;
use crate::coord::Coordinator;
use crate::error::AdapterError;
use crate::session::Session;

impl Coordinator {
    #[instrument]
    pub(crate) async fn sequence_create_metric_sink(
        &mut self,
        session: &Session,
        plan: plan::CreateMetricSinkPlan,
        resolved_ids: ResolvedIds,
    ) -> Result<ExecuteResponse, AdapterError> {
        let plan::CreateMetricSinkPlan {
            name,
            metric_sink,
            if_not_exists,
        } = plan;

        self.ensure_metric_sink_prefix_is_free(&name, metric_sink.cluster_id, &metric_sink.prefix)?;

        let (item_id, global_id) = self.allocate_user_id().await?;
        let op = catalog::Op::CreateItem {
            id: item_id,
            name: name.clone(),
            item: CatalogItem::MetricSink(MetricSink {
                create_sql: metric_sink.create_sql,
                global_id,
                from: metric_sink.from,
                resolved_ids,
                cluster_id: metric_sink.cluster_id,
                prefix: metric_sink.prefix,
                optimized_plan: None,
                physical_plan: None,
                dataflow_metainfo: None,
            }),
            owner_id: *session.current_role_id(),
        };

        match self.catalog_transact(Some(session), vec![op]).await {
            Ok(()) => Ok(ExecuteResponse::CreatedMetricSink),
            Err(AdapterError::Catalog(mz_catalog::memory::error::Error {
                kind: ErrorKind::Sql(CatalogError::ItemAlreadyExists(_, _)),
            })) if if_not_exists => {
                session.add_notice(AdapterNotice::ObjectAlreadyExists {
                    name: name.item,
                    ty: "metric sink",
                });
                Ok(ExecuteResponse::CreatedMetricSink)
            }
            Err(err) => Err(err),
        }
    }

    /// Rejects `prefix` if it is a prefix of, or has as a prefix, any metric sink already on
    /// `cluster_id`.
    ///
    /// Prefix-free, not just distinct: the published name is `prefix + metric_name`, so `a_`
    /// + `b_c` and `a_b_` + `c` both publish `a_b_c`, and Prometheus silently merges same-named
    /// families. Uniqueness only holds per cluster: the registry is process-local and every
    /// replica of a cluster runs the same sinks.
    ///
    /// This is the authoritative check, not the plan-time one. Planning is not serialized
    /// against catalog writes, so two creates can plan against the same state. The coordinator
    /// sequences one statement at a time, and nothing commits between here and
    /// `catalog_transact`.
    ///
    /// A sink already holding `name` is skipped: the create is then a no-op (`IF NOT EXISTS`)
    /// or an "already exists" error, neither of which publishes anything new.
    fn ensure_metric_sink_prefix_is_free(
        &self,
        name: &QualifiedItemName,
        cluster_id: ClusterId,
        prefix: &str,
    ) -> Result<(), AdapterError> {
        let cluster = self.catalog().get_cluster(cluster_id);
        for item_id in &cluster.bound_objects {
            let entry = self.catalog().get_entry(item_id);
            let CatalogItem::MetricSink(existing) = entry.item() else {
                continue;
            };
            if entry.name() == name {
                continue;
            }
            if existing.prefix.starts_with(prefix) || prefix.starts_with(&existing.prefix) {
                return Err(AdapterError::Unstructured(anyhow!(
                    "metric sink prefix {:?} conflicts with prefix {:?} of metric sink {} on \
                     cluster {}",
                    prefix,
                    existing.prefix,
                    entry.name().item,
                    cluster.name,
                )));
            }
        }
        Ok(())
    }
}
