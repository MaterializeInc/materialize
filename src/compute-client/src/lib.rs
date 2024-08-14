// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![warn(missing_docs)]

//! The public API for the compute layer.

pub mod as_of_selection;
pub mod controller;
pub mod logging;
pub mod metrics;
pub mod protocol;
pub mod service;
