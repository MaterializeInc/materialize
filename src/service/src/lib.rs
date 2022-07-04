// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Common code for services orchestrated by environmentd.
//!
//! This crate lacks focus. It is a hopefully short term home for code that is
//! shared between storaged and computed. The code shared here is not obviously
//! reusable by future services we may split out of environmentd. As storaged
//! and computed begin to evolve independently, we expect many of the
//! currently shared traits in this crate to evolve separate implementations in
//! the `storage` and `compute` crates. The code that remains in this crate
//! could then be moved to more focused crates, e.g., an `mz-grpc` crate for the
//! gRPC utilities that are presently in the `grpc` module.

pub mod client;
pub mod frontiers;
pub mod grpc;
pub mod local;
