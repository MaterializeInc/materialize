// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![warn(missing_docs)]

//! Shared types for the `mz-compute*` crates

pub mod config;
pub mod dataflows;
pub mod dyncfgs;
pub mod explain;
pub mod optimizer_metrics;
pub mod plan;
pub mod sinks;
pub mod sources;

/// Identifier of a compute instance.
pub type ComputeInstanceId = mz_storage_types::instances::StorageInstanceId;
