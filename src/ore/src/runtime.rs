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

//! Tokio runtime

use std::cmp;
use std::io::Result;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::runtime::{Builder, Runtime};

/// 8MiB, 4x the default in tokio: <https://docs.rs/tokio/latest/tokio/runtime/struct.Builder.html#method.thread_stack_size>
const THREAD_STACK_SIZE_DEFAULT: usize = 8 * 1024 * 1024;

/// Sets up a tokio runtime with a reasonable set of defaults.
pub fn build_tokio_runtime(
    thread_stack_size: Option<usize>,
    thread_prefix: Option<&'static str>,
) -> Result<Runtime> {
    let ncpus_useful = usize::max(1, cmp::min(num_cpus::get(), num_cpus::get_physical()));
    Builder::new_multi_thread()
        .worker_threads(ncpus_useful)
        // The default thread name exceeds the Linux limit on thread name
        // length, so pick something shorter.
        .thread_name_fn(move || {
            static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
            let id = ATOMIC_ID.fetch_add(1, Ordering::SeqCst);
            // This will wrap around eventually, which is not ideal, but it's important that
            // it stays small to fit within OS limits.
            format!("{}{}", thread_prefix.unwrap_or("tokio:work-"), id % 0x10000)
        })
        .thread_stack_size(thread_stack_size.unwrap_or(THREAD_STACK_SIZE_DEFAULT))
        .enable_all()
        .build()
}
