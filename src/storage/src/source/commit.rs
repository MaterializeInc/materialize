// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Drivers for upstream commit
use std::collections::BTreeMap;

use async_trait::async_trait;
use timely::progress::Antichain;

use mz_repr::GlobalId;
use mz_storage_client::types::sources::SourceTimestamp;

use crate::source::types::OffsetCommitter;

/// An OffsetCommitter that simply logs its callbacks.
pub struct LogCommitter {
    pub(crate) source_id: GlobalId,
    pub(crate) worker_id: usize,
    pub(crate) worker_count: usize,
}

#[async_trait]
impl<Time: SourceTimestamp> OffsetCommitter<Time> for LogCommitter {
    async fn commit_offsets(&self, offsets: Antichain<Time>) -> Result<(), anyhow::Error> {
        tracing::trace!(
            ?offsets,
            "source reader({}) \
            {}/{} received offsets to commit",
            self.source_id,
            self.worker_id,
            self.worker_count,
        );
        Ok(())
    }
}
