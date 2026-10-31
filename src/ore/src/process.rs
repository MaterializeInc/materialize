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

/// The exit code of [`halt!`](crate::halt).
pub const HALT_EXIT_CODE: i32 = 166;

#[cfg(all(feature = "antithesis", target_os = "linux"))]
#[path = "halt_evidence.rs"]
mod halt_evidence;

/// Records bounded halt evidence in Antithesis builds.
#[doc(hidden)]
pub fn record_halt(code: i32, location: &str, message: &str) {
    #[cfg(all(feature = "antithesis", target_os = "linux"))]
    halt_evidence::record(code, location, message);
    #[cfg(not(all(feature = "antithesis", target_os = "linux")))]
    let _ = (code, location, message);
}

/// The exit code of [`restart_in_place!`](crate::restart_in_place).
///
/// This is `EX_TEMPFAIL` from `sysexits.h`. Shells, tini and Kubernetes report
/// a process killed by signal N as exit code 128 + N, so a code below 128 can
/// only come from an explicit exit.
pub const RESTART_IN_PLACE_EXIT_CODE: i32 = 75;

/// Halts the process.
///
/// `halt` forwards the provided arguments to the [`tracing::warn`] macro, then
/// terminates the process with [`HALT_EXIT_CODE`].
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
#[cfg_attr(nightly_doc_features, doc(cfg(feature = "tracing")))]
#[cfg(feature = "tracing")]
#[macro_export]
macro_rules! halt {
    ($($arg:expr),* $(,)?) => {{
        let message = format!($($arg),*);
        $crate::__private::tracing::warn!("halting process: {}", message);
        $crate::antithesis_reachable!(
            ::std::concat!("process halted at ", ::std::file!(), ":", ::std::line!()),
            &message
        );
        $crate::antithesis::halted(
            ::std::concat!(::std::file!(), ":", ::std::line!()),
            &message,
        );
        $crate::process::record_halt(
            $crate::process::HALT_EXIT_CODE,
            ::std::concat!(::std::file!(), ":", ::std::line!()),
            &message,
        );
        $crate::process::exit_thread_safe($crate::process::HALT_EXIT_CODE);
    }}
}

/// Halts the process and asks its supervisor to restart it in place.
///
/// `restart_in_place` behaves like [`halt!`](crate::halt), but terminates the
/// process with [`RESTART_IN_PLACE_EXIT_CODE`]. The environmentd container
/// entrypoint restarts the process right away on this exit code instead of
/// exiting the container, which avoids Kubernetes crash backoff when a process
/// halts repeatedly.
///
/// The restarted process runs even if Kubernetes has replaced the process's pod
/// in the meantime, for example after a node failure. Only use this macro while
/// the deployment is read-only, where the entrypoint's restart marker keeps the
/// restarted process from booting writable next to the replacement. Use
/// [`halt!`](crate::halt) everywhere else, in particular after the process has
/// been fenced out.
#[cfg_attr(nightly_doc_features, doc(cfg(feature = "tracing")))]
#[cfg(feature = "tracing")]
#[macro_export]
macro_rules! restart_in_place {
    ($($arg:expr),* $(,)?) => {{
        $crate::__private::tracing::warn!("halting process for restart in place: {}", format!($($arg),*));
        $crate::process::exit_thread_safe($crate::process::RESTART_IN_PLACE_EXIT_CODE);
    }}
}

/// Prints the given message and exits the process.
///
/// `exit!` forwards the provided arguments to the [`tracing::info`] macro, then
/// terminates the process with given exit code.
#[cfg_attr(nightly_doc_features, doc(cfg(feature = "tracing")))]
#[cfg(feature = "tracing")]
#[macro_export]
macro_rules! exit {
    ($exit_code:literal, $($arg:expr),* $(,)?) => {{
        $crate::__private::tracing::info!("exiting process (0): {}", format!($($arg),*));
        $crate::process::exit_thread_safe($exit_code);
    }};
}

/// Like [`halt!`], but exits with the given code, so that a restart with a
/// specific cause can be told apart from a routine halt.
#[cfg_attr(nightly_doc_features, doc(cfg(feature = "tracing")))]
#[cfg(feature = "tracing")]
pub fn halt_with_code(exit_code: i32, message: std::fmt::Arguments<'_>) -> ! {
    crate::__private::tracing::warn!("halting process: {message}");
    exit_thread_safe(exit_code)
}

/// Helper for exiting macros.
///
/// This function exists to avoid that all callers of `halt!` have to explicitly depend on `libc`.
pub fn exit_thread_safe(error_code: i32) -> ! {
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
    unsafe { libc::_exit(error_code) };
}

/// Sets the OS-level name of the calling thread, visible to tools like `top -H`, `perf`,
/// `samply`, and `/proc/<pid>/task/*/comm`.
///
/// Linux truncates thread names to 15 bytes (16 with the NUL terminator); `name` is truncated to
/// fit. Best-effort: a no-op on non-Linux platforms, and silently does nothing if the underlying
/// syscall fails.
pub fn set_current_thread_name(name: &str) {
    #[cfg(target_os = "linux")]
    {
        let mut truncated = name.to_string();
        while truncated.len() > 15 {
            truncated.pop();
        }
        if let Ok(name) = std::ffi::CString::new(truncated) {
            unsafe {
                libc::pthread_setname_np(libc::pthread_self(), name.as_ptr());
            }
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = name;
    }
}
