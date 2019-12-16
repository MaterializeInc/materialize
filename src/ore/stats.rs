// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! Statistics utilities.

/// Buckets that can capture data between one microsecond and 1 second.
pub const HISTOGRAM_BUCKETS: [f64; 17] = [
    0.000_016, 0.000_032, 0.000_064, 0.000_128, 0.000_256, 0.000_512, 0.001, 0.002, 0.004, 0.008,
    0.016, 0.032, 0.064, 0.128, 0.256, 0.512, 1.0,
];
