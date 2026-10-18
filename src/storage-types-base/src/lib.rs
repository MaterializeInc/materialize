// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Plain data types of storage connections and sinks.
//!
//! This crate must not depend on clients for external systems (Kafka, AWS, databases), so that
//! crates naming these types, such as `mz-compute-types`, do not pull those clients in.
//! `mz-storage-types` re-exports the types at their usual paths and owns their behavior.

pub mod connections;
pub mod sinks;
