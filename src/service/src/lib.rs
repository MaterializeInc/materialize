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
//! This crate lacks focus. It is a hopefully short term home for code that was
//! previously shared between storaged and computed. The code shared here is not
//! obviously reusable by future services we may split out of environmentd.

pub mod boot;
pub mod client;
pub mod grpc;
pub mod local;
pub mod params;
pub mod retry;
pub mod secrets;
pub mod tracing;
pub mod transport;
