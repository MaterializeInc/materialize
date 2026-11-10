// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Shared compute query and planning errors.

use mz_compute_types::ComputeInstanceId;
use thiserror::Error;

pub use mz_storage_types::errors::CollectionMissing;

/// The error returned by replica-targeted peeks and subscribes when the target replica
/// disconnects.
pub const ERROR_TARGET_REPLICA_FAILED: &str = "target replica failed or was dropped";

/// Error returned in response to a reference to an unknown compute instance.
#[derive(Error, Debug)]
#[error("instance does not exist: {0}")]
pub struct InstanceMissing(pub ComputeInstanceId);
