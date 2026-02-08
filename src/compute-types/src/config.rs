// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Compute configuration types.

use serde::{Deserialize, Serialize};

pub use mz_catalog_types::ComputeReplicaLogging;

/// Replica configuration
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ComputeReplicaConfig {
    /// TODO(database-issues#7533): Add documentation.
    pub logging: ComputeReplicaLogging,
}
