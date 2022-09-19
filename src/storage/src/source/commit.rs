// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Drivers for upstream commit
use std::collections::HashMap;
use std::marker::{Send, Sync};

use async_trait::async_trait;
use tokio::sync::watch;

use mz_expr::PartitionId;
use mz_ore::task;
use mz_repr::GlobalId;

use crate::source::types::OffsetCommitter;
use crate::types::sources::MzOffset;

/// An OffsetCommitter that simply logs its callbacks.
pub struct LogCommitter {
    pub(crate) source_id: GlobalId,
    pub(crate) worker_id: usize,
    pub(crate) worker_count: usize,
}

#[async_trait]
impl OffsetCommitter for LogCommitter {
    async fn commit_offsets(
        &self,
        offsets: HashMap<PartitionId, MzOffset>,
    ) -> Result<(), anyhow::Error> {
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

pub(crate) struct OffsetCommitHandle {
    sender: watch::Sender<HashMap<PartitionId, MzOffset>>,
}

impl OffsetCommitHandle {
    pub(crate) fn commit_offsets(&self, offsets: HashMap<PartitionId, MzOffset>) {
        self.sender
            .send(offsets)
            .expect("the receiver to drop first")
    }
}

pub(crate) fn drive_offset_committer<S: OffsetCommitter + Send + Sync + 'static>(
    sc: S,
    source_id: GlobalId,
    worker_id: usize,
    worker_count: usize,
) -> OffsetCommitHandle {
    let (tx, mut rx): (_, watch::Receiver<HashMap<PartitionId, MzOffset>>) =
        watch::channel(Default::default());
    let _ = task::spawn(
        || format!("offset commiter({source_id}) {worker_id}/{worker_count}"),
        async move {
            let mut last_offsets: HashMap<PartitionId, MzOffset> = HashMap::new();
            // loop waiting on changes. Note we could miss updates,
            // but this is fine: we work on committing of offsets
            // as fast as the `OffsetCommitter` allows us.
            while let Ok(()) = rx.changed().await {
                // Clone out of the watch to avoid holding the read lock
                // for longer that necessary.
                let new_offsets: HashMap<PartitionId, MzOffset> = {
                    let new_offsets = rx.borrow();
                    new_offsets.clone()
                };

                // If we actually have new offsets, and they aren't exactly the same
                // as the previous ones we tried (we don't attempt any partial ordering
                // here), then we commit them, logging errors.
                //
                // TODO(guswynn): only push updates.
                if !new_offsets.is_empty()
                    && (last_offsets.is_empty() || last_offsets != new_offsets)
                {
                    last_offsets = new_offsets.clone();
                    if let Err(e) = sc.commit_offsets(new_offsets).await {
                        // TODO(guswynn): stats for this error
                        tracing::error!(
                            %e,
                            "Failed to commit offsets for {source_id} ({worker_id}/{worker_count}"
                        );
                    }
                }
            }

            // Error's mean the send side has dropped, so we silently shutdown.
        },
    );

    OffsetCommitHandle { sender: tx }
}
