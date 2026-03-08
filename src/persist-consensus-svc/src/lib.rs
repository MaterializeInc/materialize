// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A group commit consensus service for Materialize persist.
//!
//! Batches independent cross-shard CAS writes into a single durable S3 Express
//! One Zone PUT per flush interval, making cost O(1/batch_window) instead of
//! O(shards).

pub mod actor;
pub mod metrics;
pub mod service;
pub mod wal;
