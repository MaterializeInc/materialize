// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! This module contains the bits of SQL sessions that are required for the SQL
//! layer.
//!
//! The split between this module and the `mz_adapter::session` is organic and
//! should be revisited with more intention in the future.

pub mod hint;
pub mod user;
pub mod vars;
