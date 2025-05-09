// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Shared Storage dataflow operators

pub mod metrics;
pub mod oneshot_source;
pub mod persist_source;
pub mod s3_oneshot_sink;
pub mod stats;
