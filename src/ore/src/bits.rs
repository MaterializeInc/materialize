// Copyright Materialize, Inc. and contributors. All rights reserved.
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

//! Utilities for bit and byte manipulation

/// Increases `p` as little as possible (including possibly 0)
/// such that it becomes a multiple of `N`.
pub const fn align_up<const N: usize>(p: usize) -> usize {
    if p % N == 0 {
        p
    } else {
        p + (N - (p % N))
    }
}
