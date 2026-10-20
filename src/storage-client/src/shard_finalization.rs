// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Runtime ordering between txn-WAL retirement and physical shard closure.

use std::collections::BTreeSet;
use std::fmt::Debug;
use std::sync::{Arc, Mutex};

use futures::future::BoxFuture;
use mz_persist_client::ShardId;

/// Completes shared txn-WAL retirement of catalog-certified unreferenced shards.
///
/// Success must mean Forget completed definitively, including applying preceding
/// transactions. Local detachment, cancellation or a lost acknowledgement cannot
/// satisfy this contract. This does not confer authority to submit user writes.
pub trait ShardRetirement: Debug + Send + Sync {
    fn forget(&self, shards: BTreeSet<ShardId>) -> BoxFuture<'static, anyhow::Result<()>>;
}

/// Supplies the runtime prerequisite for physical finalization.
///
/// The finalizer must remain inactive until the table writer is ready. Installing
/// this after bootstrap prevents retirement from overtaking bootstrap registration.
/// It is not a membership registry or a durable reclamation protocol.
#[derive(Clone, Debug, Default)]
pub struct ShardFinalization {
    retirement: Arc<Mutex<Option<Arc<dyn ShardRetirement>>>>,
}

impl ShardFinalization {
    /// Installs the writer's retirement path after bootstrap completes.
    pub fn install(&self, retirement: Arc<dyn ShardRetirement>) {
        let mut slot = self.retirement.lock().expect("lock poisoned");
        assert!(slot.is_none(), "retirement path already installed");
        *slot = Some(retirement);
    }

    pub(crate) fn retirement(&self) -> Option<Arc<dyn ShardRetirement>> {
        self.retirement.lock().expect("lock poisoned").clone()
    }
}
