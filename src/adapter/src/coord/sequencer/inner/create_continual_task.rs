// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_ore::instrument;
use mz_sql::names::ResolvedIds;
use mz_sql::plan;

use crate::command::ExecuteResponse;
use crate::coord::Coordinator;
use crate::error::AdapterError;
use crate::session::Session;

impl Coordinator {
    #[instrument]
    pub(crate) async fn sequence_create_continual_task(
        &mut self,
        session: &Session,
        plan: plan::CreateContinualTaskPlan,
        resolved_ids: ResolvedIds,
    ) -> Result<ExecuteResponse, AdapterError> {
        todo!("WIP {:?}", (session, plan, resolved_ids));
    }
}
