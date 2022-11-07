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

//! Process utilities.

use std::sync::atomic::AtomicBool;

/// Halts the process.
///
/// `halt` forwards the provided arguments to the [`tracing::warn`] macro, then
/// terminates the process with exit code 2.
///
/// Halting a process is a middle ground between a graceful shutdown and a
/// panic. Use halts for errors that are severe enough to require shutting down
/// the process, but not severe enough to be considered a crash. Halts are not
/// intended to be reported to error tracking tools (e.g., Sentry).
///
/// There are two common classes of errors that trigger a halt:
///
///   * An error that indicates a new process has taken over (e.g., an error
///     indicating that the process's leader epoch has expired).
///
///   * A retriable error where granular retry logic would be tricky enough to
///     write that it has not yet been worth it. Since restarting from a fresh
///     process must *always* be correct, forcing a restart is an easy way to
///     tap into the existing whole-process retry logic. Use halt judiciously in
///     these cases, as restarting a process can be inefficient (e.g., mandatory
///     restart backoff, rehydrating expensive state).
///
/// If you need to write a Rust libtest test that asserts that a call to halt
/// has occcurred, see [`PANIC_ON_HALT`].
#[cfg_attr(nightly_doc_features, doc(cfg(feature = "tracing")))]
#[cfg(feature = "tracing")]
#[macro_export]
macro_rules! halt {
    ($($arg:expr),* $(,)?) => {{
        $crate::__private::tracing::warn!("halting process: {}", format!($($arg),*));
        if $crate::process::PANIC_ON_HALT.load(std::sync::atomic::Ordering::SeqCst) {
            panic!("promoting halt to panic because PANIC_ON_HALT is set");
        } else {
            ::std::process::exit(2);
        }
    }}
}

/// Override the behavior of [`halt`] to panic rather than exiting the process.
///
/// Intended for use in Rust libtest tests which want to assert that a halt has
/// occurred via [`std::panic::catch_unwind`]. The default behavior exits the
/// test binary itself, which is not testable from within the same process.
pub static PANIC_ON_HALT: AtomicBool = AtomicBool::new(false);
