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
#[cfg_attr(nightly_doc_features, doc(cfg(feature = "tracing_")))]
#[cfg(feature = "tracing_")]
#[macro_export]
macro_rules! halt {
    ($($arg:expr),* $(,)?) => {{
        $crate::__private::tracing::warn!("halting process: {}", format!($($arg),*));
        $crate::process::halt();
    }}
}

/// Helper for the `halt!` macro.
///
/// This function exists to avoid that all callers of `halt!` have to explicitly depend on `libc`.
pub fn halt() -> ! {
    // Using `std::process::exit` here would be unsound as that function invokes libc's `exit`
    // function, which is not thread-safe [1]. There are two viable work arounds for this:
    //
    //  * Calling libc's `_exit` function instead, which is thread-safe by virtue of not performing
    //    any cleanup work.
    //  * Introducing a global lock to ensure only a single thread gets to call `exit` at a time.
    //
    // We chose the former approach because it's simpler and has the additional benefit of
    // protecting us from third-party code that installs thread-unsafe exit handlers.
    //
    // Note that we are fine with not running any cleanup code here. This behaves just like an
    // abort we'd do in response to a panic. Any code that relies on cleanup code to run before
    // process termination is already unsound in the face of panics and hardware failure.
    //
    // [1]: https://github.com/rust-lang/rust/issues/126600
    unsafe { libc::_exit(2) };
}
