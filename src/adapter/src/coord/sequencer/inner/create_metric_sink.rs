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

use mz_catalog::memory::error::ErrorKind;
use mz_catalog::memory::objects::{CatalogItem, MetricSink};
use mz_ore::instrument;
use mz_sql::catalog::CatalogError;
use mz_sql::names::ResolvedIds;
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
}
