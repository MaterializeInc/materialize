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

//! Panic utilities.

use std::panic;
use std::process;

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
/// Computations in which a panic is expected can still use
/// [`panic::catch_unwind`] to recover.
pub fn set_abort_on_panic() {
    let old_hook = panic::take_hook();
    panic::set_hook(Box::new(move |panic_info| {
        old_hook(panic_info);
        process::abort();
    }))
}
