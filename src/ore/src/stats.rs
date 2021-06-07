// Copyright Materialize, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Statistics utilities.

/// Buckets that can capture data between one microsecond and 1 second.
pub const HISTOGRAM_BUCKETS: [f64; 17] = [
    0.000_016, 0.000_032, 0.000_064, 0.000_128, 0.000_256, 0.000_512, 0.001, 0.002, 0.004, 0.008,
    0.016, 0.032, 0.064, 0.128, 0.256, 0.512, 1.0,
];
