// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.
use std::str::FromStr;

use http::Uri;

use mz_repr::{Datum, RowArena};
use mz_sql::plan::{self, CopyToPlan};

use crate::coord::{Coordinator, TargetCluster};
use crate::optimize::dataflows::{prep_scalar_expr, EvalTime, ExprPrepStyle};
use crate::session::Session;
use crate::{AdapterError, ExecuteResponse};

impl Coordinator {
    #[tracing::instrument(level = "debug", skip(self))]
    pub(crate) async fn sequence_copy_to(
        &mut self,
        session: &Session,
        plan: plan::CopyToPlan,
        target_cluster: TargetCluster,
    ) -> Result<ExecuteResponse, AdapterError> {
        let CopyToPlan {
            from: _from,
            mut to,
            connection: _connection,
            format_params: _format_params,
        } = plan;

        let style = ExprPrepStyle::OneShot {
            logical_time: EvalTime::NotAvailable,
            session,
            catalog_state: self.catalog().state(),
        };

        prep_scalar_expr(&mut to, style)?;
        let temp_storage = RowArena::new();
        let evaled = to.eval(&[], &temp_storage)?;
        if evaled == Datum::Null {
            coord_bail!("COPY TO target value can not be null");
        }
        let to_url = match Uri::from_str(evaled.unwrap_str()) {
            Ok(url) => {
                if url.scheme_str() != Some("s3") {
                    coord_bail!("only 's3://...' urls are supported as COPY TO target");
                }
                url
            }
            Err(e) => coord_bail!("could not parse COPY TO target url: {}", e),
        };

        // TODO(mouli): Implement this
        Err(AdapterError::Internal(format!(
            "COPY TO '{}' is not yet implemented",
            to_url,
        )))
    }
}
