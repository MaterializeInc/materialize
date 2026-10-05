// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Description of how a dataflow follows wall-clock time, independent of a specific point in time.

pub use mz_repr::time_dependence::TimeDependence;
use thiserror::Error;

use crate::instances::StorageInstanceId;

/// Errors arising when reading time dependence information.
#[derive(Error, Debug)]
pub enum TimeDependenceError {
    /// The given instance does not exist.
    #[error("instance does not exist: {0}")]
    InstanceMissing(StorageInstanceId),
    /// One of the imported collections does not exist.
    #[error("collection does not exist: {0}")]
    CollectionMissing(mz_repr::GlobalId),
}
