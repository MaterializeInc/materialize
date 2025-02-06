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

//! Panic utilities.

use std::any::Any;
use std::backtrace::Backtrace;
use std::borrow::Cow;
use std::cell::RefCell;
use std::fs::File;
use std::io::{self, Write as _};
use std::os::fd::FromRawFd;
use std::panic::{self, UnwindSafe};
use std::process;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::{env, thread};

use chrono::Utc;
use itertools::Itertools;
#[cfg(feature = "async")]
use tokio::task_local;

use crate::iter::IteratorExt;

thread_local! {
    static CATCHING_UNWIND: RefCell<bool> = const { RefCell::new(false) };
}

#[cfg(feature = "async")]
task_local! {
    pub(crate) static CATCHING_UNWIND_ASYNC: bool;
}

/// Overwrites the default panic handler with an enhanced panic handler.
///
/// The enhanced panic handler:
///
///   * Always emits a backtrace, regardless of how `RUST_BACKTRACE` was
///     configured.
///
///   * Writes to stderr as atomically as possible, to minimize interleaving
///     with concurrent log messages.
///
///   * Instructs the entire process to abort if any thread panics.
///
///     By default, when a thread panics in Rust, only that thread is affected,
///     and other threads continue running unaffected. This is a bad default. In
///     almost all programs, thread panics are unexpected, unrecoverable, and
///     leave the overall program in an invalid state. It is therefore typically
///     less confusing to abort the entire program.
///
///     For example, consider a simple program with two threads communicating
///     through a channel, where the first thread is waiting for the second
///     thread to send a value over the channel. If the second thread panics,
///     the first thread will block forever for a value that will never be
///     produced. Blocking forever will be more confusing to the end user than
///     aborting the program entirely.
///
/// Note that after calling this function, computations in which a panic is
/// expected must use the special [`catch_unwind`] function in this module to
/// recover. Note that the `catch_unwind` function in the standard library is
/// **not** compatible with this improved panic handler.
pub fn install_enhanced_handler() {
    panic::set_hook(Box::new(move |panic_info| {
        // If we're catching an unwind, do nothing to let the unwind handler
        // run.
        let catching_unwind = CATCHING_UNWIND.with(|v| *v.borrow());
        #[cfg(feature = "async")]
        let catching_unwind_async = CATCHING_UNWIND_ASYNC.try_with(|v| *v).unwrap_or(false);
        #[cfg(not(feature = "async"))]
        let catching_unwind_async = false;
        if catching_unwind || catching_unwind_async {
            return;
        }

        let timestamp = Utc::now().format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string();
        let thread = thread::current();
        let thread_name = thread.name().unwrap_or("<unnamed>");

        let msg = match panic_info.payload().downcast_ref::<&'static str>() {
            Some(s) => *s,
            None => match panic_info.payload().downcast_ref::<String>() {
                Some(s) => &s[..],
                None => "Box<Any>",
            },
        };

        let location = if let Some(loc) = panic_info.location() {
            loc.to_string()
        } else {
            "<unknown>".to_string()
        };

        // We unconditionally collect and display a short backtrace, as there's
        // no practical situation where producing a backtrace in a panic message
        // is undesirable. Panics are always unexpected, and we don't want to
        // miss our chance to give ourselves as much context as possible.
        //
        // We do support `RUST_BACKTRACE=full` to display a full backtrace
        // rather than a short backtrace that omits the frames from the runtime
        // and the panic handler itslef.
        let mut backtrace = Backtrace::force_capture().to_string();
        if env::var("RUST_BACKTRACE").as_deref() != Ok("full") {
            // Rust doesn't provide an API for generating a short backtrace, so
            // we have to string munge it ourselves. The relevant frames are
            // between the call to `backtrace::__rust_begin_short_backtrace` and
            // `backtrace::__rust_end_short_backtrace`, which are easy to sniff
            // out. To make this string munging as robust as possible, if we
            // don't find the first marker frame, we  leave the full backtrace
            // in place.
            let mut lines = backtrace.lines();
            if lines
                .find(|l| l.contains("backtrace::__rust_end_short_backtrace"))
                .is_some()
            {
                lines.next();
                backtrace = lines
                    .take_while(|l| !l.contains("backtrace::__rust_begin_short_backtrace"))
                    .chain_one("note: Some details are omitted, run with `RUST_BACKTRACE=full` for a verbose backtrace.\n")
                    .join("\n");
            }
        };

        // Rust uses an unbuffered stderr stream. Build the message in a buffer
        // first so that we minimize the number of calls to the underlying
        // `write(2)` system call. This minimizes the chance of (but does not
        // outright prevent) interleaving output with C or C++ libraries that
        // may be writing to the stderr stream outside of the Rust runtime.
        //
        // See https://github.com/rust-lang/rust/issues/64413 for details.
        let buf = format!(
            "{timestamp}  thread '{thread_name}' panicked at {location}:\n{msg}\n{backtrace}"
        );

        // Ideal path: spawn a thread that attempts to lock the Rust-managed
        // stderr stream and write the panic message there. Acquiring the stderr
        // lock prevents interleaving with concurrent output from the `tracing`
        // crate, because `tracing` also acquires the lock before printing
        // output.
        //
        // We put the ideal path in a thread because there is no guarantee that
        // we'll be able to acquire the lock in a timely fashion, and there is
        // no API to specify a time out for the lock call.
        let buf = Arc::new(Mutex::new(Some(buf)));
        thread::spawn({
            let buf = Arc::clone(&buf);
            move || {
                let mut stderr = io::stderr().lock();
                let mut buf = buf.lock().unwrap();
                if let Some(buf) = buf.take() {
                    let _ = stderr.write_all(buf.as_bytes());
                }

                // Abort while still holding the stderr lock to ensure the panic
                // is the last output printed to stderr.
                process::abort();
            }
        });

        // Backup path: wait one second for the ideal path to succeed, then
        // write the panic message directly to the underlying stderr stream
        // (file descriptor 2) if it wasn't already written by the ideal path.
        // This ensures we eventually eke out a panic message, possibly
        // interleaved with other output, even if another thread is wedged while
        // holding the stderr lock.
        thread::sleep(Duration::from_secs(1));
        let mut buf = buf.lock().unwrap();
        if let Some(buf) = buf.take() {
            let mut stderr = unsafe { File::from_raw_fd(2) };
            let _ = stderr.write_all(buf.as_bytes());
        }

        process::abort();
    }))
}

/// Like [`std::panic::catch_unwind`], but can unwind panics even if
/// [`install_enhanced_handler`] has been called.
pub fn catch_unwind<F, R>(f: F) -> Result<R, Box<dyn Any + Send + 'static>>
where
    F: FnOnce() -> R + UnwindSafe,
{
    CATCHING_UNWIND.with(|catching_unwind| {
        *catching_unwind.borrow_mut() = true;
        #[allow(clippy::disallowed_methods)]
        let res = panic::catch_unwind(f);
        *catching_unwind.borrow_mut() = false;
        res
    })
}

/// Like [`crate::panic::catch_unwind`], but downcasts the returned `Box<dyn Any>` error to a
/// string which is almost always is.
///
/// See: <https://doc.rust-lang.org/stable/std/panic/struct.PanicHookInfo.html#method.payload>
pub fn catch_unwind_str<F, R>(f: F) -> Result<R, Cow<'static, str>>
where
    F: FnOnce() -> R + UnwindSafe,
{
    match crate::panic::catch_unwind(f) {
        Ok(res) => Ok(res),
        Err(opaque) => match opaque.downcast_ref::<&'static str>() {
            Some(s) => Err(Cow::Borrowed(*s)),
            None => match opaque.downcast_ref::<String>() {
                Some(s) => Err(Cow::Owned(s.to_owned())),
                None => Err(Cow::Borrowed("Box<Any>")),
            },
        },
    }
}
