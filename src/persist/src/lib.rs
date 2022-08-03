// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Persistence for differential dataflow collections

#![warn(missing_docs, missing_debug_implementations)]
#![warn(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::clone_on_ref_ptr
)]

pub mod cfg;
pub mod error;
pub mod file;
pub mod gen;
pub mod indexed;
pub mod location;
pub mod mem;
pub mod metrics;
pub mod postgres;
pub mod retry;
pub mod s3;
pub mod unreliable;
pub mod workload;
