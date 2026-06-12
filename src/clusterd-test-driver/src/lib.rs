// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Headless frontend to `clusterd` for scripted compute tests.
//!
//! See `doc/developer/design/20260612_headless_clusterd_test_driver.md`.

pub mod ctp;
pub mod data;
pub mod dataflow;
pub mod driver;
pub mod persist_host;
pub mod responses;
pub mod script;
pub mod target;
