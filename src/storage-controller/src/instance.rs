// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Passive storage cluster inventory.

use mz_cluster_client::ReplicaId;
use std::collections::BTreeSet;

#[derive(Debug)]
pub(crate) struct Instance {
    pub workload_class: Option<String>,
    replicas: BTreeSet<ReplicaId>,
}

impl Instance {
    pub fn new(workload_class: Option<String>) -> Self {
        Self {
            workload_class,
            replicas: BTreeSet::new(),
        }
    }

    pub fn replica_ids(&self) -> impl Iterator<Item = ReplicaId> + '_ {
        self.replicas.iter().copied()
    }

    pub fn register_replica(&mut self, id: ReplicaId) {
        self.replicas.insert(id);
    }

    pub fn drop_replica(&mut self, id: ReplicaId) {
        self.replicas.remove(&id);
    }
}
