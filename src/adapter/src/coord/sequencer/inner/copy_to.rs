// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_sql::plan::{self};

use crate::coord::{Coordinator, TargetCluster};
use crate::{AdapterError, ExecuteContext};

impl Coordinator {
    #[tracing::instrument(level = "debug", skip(self))]
    pub(crate) async fn sequence_copy_to(
        &mut self,
        ctx: ExecuteContext,
        plan: plan::CopyToPlan,
        target_cluster: TargetCluster,
    ) {
        let result = Err(AdapterError::Internal(
            "COPY TO <url> is not yet implemented".to_string(),
        ));
        ctx.retire(result);
    }
}
