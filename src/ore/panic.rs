// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

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
