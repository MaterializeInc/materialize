// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Retry utilities.

/// The number of retries required before upgrading a log level from `debug`
/// to `info`.
pub const INFO_MIN_RETRIES: usize = 5;
