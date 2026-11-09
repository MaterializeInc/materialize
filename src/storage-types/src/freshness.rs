// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Wall-clock staleness histograms of collections, bucketed exponentially.

mod histogram;
mod recorder;
mod reflected;
mod reporter;

pub use histogram::BucketScheme;
pub use recorder::{PeriodRow, RecorderConfig, StalenessRecorder};
pub use reflected::reflected_time;
pub use reporter::StalenessReporter;

#[cfg(test)]
mod tests;
