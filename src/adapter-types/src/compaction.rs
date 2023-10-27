// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// TODO: We can have only two consts here, instead of three, once there exists a `const` way to
// convert between a `Timestamp` and a `Duration`, and unwrap a result in const contexts. Currently
// unstable compiler features that would allow this are:
// * `const_option`: https://github.com/rust-lang/rust/issues/67441
// * `const_result`: https://github.com/rust-lang/rust/issues/82814
// * `const_num_from_num`: https://github.com/rust-lang/rust/issues/87852
// * `const_precise_live_drops`: https://github.com/rust-lang/rust/issues/73255

use mz_repr::Timestamp;
use std::time::Duration;

/// `DEFAULT_LOGICAL_COMPACTION_WINDOW`, in milliseconds.
/// The default is set to a second to track the default timestamp frequency for sources.
const DEFAULT_LOGICAL_COMPACTION_WINDOW_MILLIS: u64 = 1000;

/// The default logical compaction window for new objects
pub const DEFAULT_LOGICAL_COMPACTION_WINDOW: Duration =
    Duration::from_millis(DEFAULT_LOGICAL_COMPACTION_WINDOW_MILLIS);

/// `DEFAULT_LOGICAL_COMPACTION_WINDOW` as an `EpochMillis` timestamp
pub const DEFAULT_LOGICAL_COMPACTION_WINDOW_TS: Timestamp =
    Timestamp::new(DEFAULT_LOGICAL_COMPACTION_WINDOW_MILLIS);
