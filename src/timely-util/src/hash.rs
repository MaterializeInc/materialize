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

//! Deterministic hashing helpers.

/// A fixed-seed [`ahash::RandomState`].
///
/// We fix the seeds explicitly rather than letting `ahash` randomize per process
/// so that hashing is deterministic across runs, replicas, and — crucially —
/// across builds that select `ahash`'s compile-time features differently. Cargo
/// unions features across the workspace, so a `RandomState::new()` could pick a
/// different hasher depending on what other dependencies enable; pinning the
/// seeds avoids that. The seeds are the ones `ahash::AHasher::default()` would
/// use. (Depending on target features `ahash` may fall back to its non-AES
/// hasher, which is still sufficient for our needs.)
///
/// Used wherever Materialize needs a stable hash: distributing data to workers
/// during consolidation ([`crate::operator`]) and the per-column codec summaries
/// in `mz_row_spine`.
pub fn fixed_state() -> ahash::RandomState {
    ahash::RandomState::with_seeds(
        0x243f_6a88_85a3_08d3,
        0x1319_8a2e_0370_7344,
        0xa409_3822_299f_31d0,
        0x082e_fa98_ec4e_6c89,
    )
}
