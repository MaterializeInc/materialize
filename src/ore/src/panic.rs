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
use std::cell::RefCell;
use std::panic::{self, UnwindSafe};
use std::process;

#[cfg(feature = "task")]
use tokio::task_local;

thread_local! {
    static CATCHING_UNWIND: RefCell<bool> = RefCell::new(false);
}

#[cfg(feature = "task")]
task_local! {
    pub(crate) static CATCHING_UNWIND_ASYNC: bool;
}

/// Instructs the entire process to abort if any thread panics.
///
/// By default, when a thread panics in Rust, only that thread is affected, and
/// other threads continue running unaffected. This is a bad default. In almost
/// all programs, thread panics are unexpected, unrecoverable, and leave the
/// overall program in an invalid state. It is therefore typically less
/// confusing to abort the entire program.
///
/// For example, consider a simple program with two threads communicating
/// through a channel, where the first thread is waiting for the second thread
/// to send a value over the channel. If the second thread panics, the first
/// thread will block forever for a value that will never be produced. Blocking
/// forever will be more confusing to the end user than aborting the program
/// entirely.
///
/// Computations in which a panic is expected can use the special
/// [`catch_unwind`] function in this module to recover. Note that the
/// `catch_unwind` function in the standard library is **not** compatible with
/// this function.
pub fn set_abort_on_panic() {
    let old_hook = panic::take_hook();
    panic::set_hook(Box::new(move |panic_info| {
        old_hook(panic_info);
        let catching_unwind = CATCHING_UNWIND.with(|v| *v.borrow());
        #[cfg(feature = "task")]
        let catching_unwind_async = CATCHING_UNWIND_ASYNC.try_with(|v| *v).unwrap_or(false);
        #[cfg(not(feature = "task"))]
        let catching_unwind_async = false;
        if !catching_unwind && !catching_unwind_async {
            process::abort();
        }
    }))
}

/// Like [`std::panic::catch_unwind`], but can unwind panics even if
/// [`set_abort_on_panic`] has been called.
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

#[cfg(test)]
mod tests {
    use std::panic;

    use scopeguard::defer;

    use super::*;

    #[test]
    fn catch_panic() {
        let old_hook = panic::take_hook();
        defer! {
            panic::set_hook(old_hook);
        }

        set_abort_on_panic();

        let result = catch_unwind(|| {
            panic!("panicked");
        })
        .unwrap_err()
        .downcast::<&str>()
        .unwrap();

        assert_eq!(*result, "panicked");
    }
}
