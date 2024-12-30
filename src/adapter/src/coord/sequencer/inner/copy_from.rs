// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_repr::{Datum, GlobalId, RowArena};
use mz_sql::plan::{self, CopyFromSource, HirScalarExpr};
use mz_storage_types::oneshot_sources::OneshotIngestionRequest;
use url::Url;

use crate::coord::sequencer::inner::return_if_err;
use crate::coord::{Coordinator, TargetCluster};
use crate::optimize::dataflows::{prep_scalar_expr, EvalTime, ExprPrepStyle};
use crate::{AdapterError, ExecuteContext, ExecuteResponse};

impl Coordinator {
    pub(crate) async fn sequence_copy_from(
        &mut self,
        ctx: &ExecuteContext,
        plan: plan::CopyFromPlan,
        target_cluster: TargetCluster,
    ) -> Result<ExecuteResponse, AdapterError> {
        let plan::CopyFromPlan {
            id,
            source,
            columns,
            params,
        } = plan;

        let from_expr = match source {
            CopyFromSource::Url(from_expr) => from_expr,
            CopyFromSource::Stdin => coord_bail!("COPY FROM STDIN should be handled elsewhere"),
        };

        let eval_url = |from: HirScalarExpr| -> Result<Url, AdapterError> {
            let style = ExprPrepStyle::OneShot {
                logical_time: EvalTime::NotAvailable,
                session: ctx.session(),
                catalog_state: self.catalog().state(),
            };
            let mut from = from.lower_uncorrelated()?;
            prep_scalar_expr(&mut from, style)?;

            let temp_storage = RowArena::new();
            let eval_result = from.eval(&[], &temp_storage)?;

            if eval_result == Datum::Null {
                coord_bail!("COPY FROM target value cannot be NULL");
            }
            let eval_string = match eval_result {
                Datum::Null => coord_bail!("COPY FROM target value cannot be NULL"),
                Datum::String(url_str) => url_str,
                other => coord_bail!("programming error! COPY FROM target cannot be {other}"),
            };

            Url::parse(eval_string)
                .map_err(|err| AdapterError::Unstructured(anyhow::anyhow!("{err}")))
        };
        let url = eval_url(from_expr)?;

        let dest_table = self
            .catalog()
            .get_entry(&id)
            .table()
            .expect("TODO SHOULD BE A TABLE");

        let collection_id = dest_table.global_id_writes();
        // TODO(parkmycar).
        let ingestion_id = GlobalId::Transient(100000);
        let request = OneshotIngestionRequest {
            source: mz_storage_types::oneshot_sources::ContentSource::Http { url },
            format: mz_storage_types::oneshot_sources::ContentFormat::Csv,
        };

        let target_cluster = self
            .catalog()
            .resolve_target_cluster(target_cluster, ctx.session())?;

        self.controller
            .storage
            .create_oneshot_ingestion(ingestion_id, collection_id, target_cluster.id, request)
            .await?;

        // TODO(parkmycar)
        Ok(ExecuteResponse::Copied(100))
    }
}
