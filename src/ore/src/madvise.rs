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

//! A trait for advising the kernel about memory access patterns.

// Re-export AccessPattern for convenience.
pub use madvise::AccessPattern;

/// A trait for advising the kernel about memory access patterns.
///
/// Types implementing this trait can hint to the operating system how their
/// underlying memory will be accessed, allowing for potential optimizations
/// such as read-ahead for sequential access.
pub trait MAdvise {
    /// Advise the kernel about the expected access pattern for this memory region.
    ///
    /// This is a best-effort hint - failures are silently ignored since madvise
    /// is purely advisory and the system can function correctly without it.
    fn madvise(&self, advice: AccessPattern);
}
