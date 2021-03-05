// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! System support functions.

use std::alloc::{self, Layout};
use std::io::{self, Write};
use std::process;
use std::ptr;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::{bail, Context};
use log::{trace, warn};
use nix::errno;
use nix::sys::signal;

#[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "ios")))]
pub fn adjust_rlimits() {
    trace!("rlimit crate does not support this OS; not adjusting nofile limit");
}

/// Attempts to increase the soft nofile rlimit to the maximum possible value.
#[cfg(any(target_os = "macos", target_os = "linux", target_os = "ios"))]
pub fn adjust_rlimits() {
    use rlimit::{Resource, Rlim};

    // getrlimit/setrlimit can have surprisingly different behavior across
    // platforms, even with the rlimit wrapper crate that we use. This function
    // is chattier than normal at the trace log level in an attempt to ease
    // debugging of such differences.

    let (soft, hard) = match Resource::NOFILE.get() {
        Ok(limits) => limits,
        Err(e) => {
            trace!("unable to read initial nofile rlimit: {}", e);
            return;
        }
    };
    trace!("initial nofile rlimit: ({}, {})", soft, hard);

    #[cfg(target_os = "macos")]
    let hard = {
        use std::cmp;
        use std::convert::TryFrom;
        use sysctl::Sysctl;

        // On macOS, getrlimit by default reports that the hard limit is
        // unlimited, but there is usually a stricter hard limit discoverable
        // via sysctl. Failing to discover this secret stricter hard limit will
        // cause the call to setrlimit below to fail.
        let res = sysctl::Ctl::new("kern.maxfilesperproc")
            .and_then(|ctl| ctl.value())
            .map_err(|e| e.to_string())
            .and_then(|v| match v {
                sysctl::CtlValue::Int(v) => usize::try_from(v)
                    .map_err(|_| format!("kern.maxfilesperproc unexpectedly negative: {}", v)),
                o => Err(format!("unexpected sysctl value type: {:?}", o)),
            });
        match res {
            Ok(v) => {
                trace!("sysctl kern.maxfilesperproc hard limit: {}", v);
                cmp::min(Rlim::from_usize(v), hard)
            }
            Err(e) => {
                trace!("error while reading sysctl: {}", e);
                hard
            }
        }
    };

    trace!("attempting to adjust nofile rlimit to ({0}, {0})", hard);
    if let Err(e) = Resource::NOFILE.set(hard, hard) {
        trace!("error adjusting nofile rlimit: {}", e);
        return;
    }

    // Check whether getrlimit reflects the limit we installed with setrlimit.
    // Some platforms will silently ignore invalid values in setrlimit.
    let (soft, hard) = match Resource::NOFILE.get() {
        Ok(limits) => limits,
        Err(e) => {
            trace!("unable to read adjusted nofile rlimit: {}", e);
            return;
        }
    };
    trace!("adjusted nofile rlimit: ({}, {})", soft, hard);

    let recommended_soft = Rlim::from_usize(1024);
    if soft < recommended_soft {
        warn!(
            "soft nofile rlimit ({}) is dangerously low; at least {} is recommended",
            soft, recommended_soft
        )
    }
}

/// Attempts to enable backtraces when SIGBUS or SIGSEGV occurs.
///
/// In particular, this means producing backtraces on stack overflow, as stack
/// overflow raises SIGBUS or SIGSEGV via guard pages. The approach here
/// involves making system calls to handle SIGBUS/SIGSEGV on an alternate signal
/// stack, which seems to work well in practice but may technically be undefined
/// behavior.
///
/// Rust may someday do this by default. Follow:
/// https://github.com/rust-lang/rust/issues/51405.
pub fn enable_sigbus_sigsegv_backtraces() -> Result<(), anyhow::Error> {
    // This code is derived from the code in the backtrace-on-stack-overflow
    // crate, which is freely available under the terms of the Apache 2.0
    // license. The modifications here provide better error messages if any of
    // the various system calls fail.
    //
    // See: https://github.com/matklad/backtrace-on-stack-overflow

    // NOTE(benesch): The stack size was chosen to match the default Rust thread
    // stack size of 2MiB. Probably overkill, but we'd much rather have
    // backtraces on stack overflow than squabble over a few megabytes. Using
    // libc::SIGSTKSZ is tempting, but its default of 8KiB on my system makes me
    // nervous. Rust code isn't used to running with a stack that small.
    const STACK_SIZE: usize = 2 << 20;

    // x86_64 and aarch64 require 16-byte alignment. Its hard to imagine other
    // platforms that would have more stringent requirements.
    const STACK_ALIGN: usize = 16;

    // Allocate a stack.
    let buf_layout =
        Layout::from_size_align(STACK_SIZE, STACK_ALIGN).expect("layout known to be valid");
    // SAFETY: layout has non-zero size and the uninitialized memory that is
    // returned is never read (at least, not by Rust).
    let buf = unsafe { alloc::alloc(buf_layout) };

    // Request that signals be delivered to this alternate stack.
    let stack = libc::stack_t {
        ss_sp: buf as *mut libc::c_void,
        ss_flags: 0,
        ss_size: STACK_SIZE,
    };
    // SAFETY: `stack` is a valid pointer to a `stack_t` object and the second
    // parameter, `old_ss`, is permitted to be `NULL` according to POSIX.
    let ret = unsafe { libc::sigaltstack(&stack, ptr::null_mut()) };
    if ret == -1 {
        let errno = errno::from_i32(errno::errno());
        bail!("failed to configure alternate signal stack: {}", errno);
    }

    // Install a handler for SIGSEGV.
    let action = signal::SigAction::new(
        signal::SigHandler::Handler(handle_sigbus_sigsegv),
        signal::SaFlags::SA_NODEFER | signal::SaFlags::SA_ONSTACK,
        signal::SigSet::empty(),
    );
    // SAFETY: see `handle_sigbus_sigsegv`.
    unsafe { signal::sigaction(signal::SIGBUS, &action) }
        .context("failed to install SIGBUS handler")?;
    unsafe { signal::sigaction(signal::SIGSEGV, &action) }
        .context("failed to install SIGSEGV handler")?;

    Ok(())
}

extern "C" fn handle_sigbus_sigsegv(_: i32) {
    // SAFETY: this is is a signal handler function and technically must be
    // "async-signal safe" [0]. That typically means no memory allocation, which
    // means no panics or backtraces... but if we're here, we're already doomed
    // by a segfault. So there is little harm to ignoring the rules and
    // panicking. If we're successful, as we often are, the panic will be caught
    // by our panic handler and displayed nicely with a backtrace that traces
    // *through* the signal handler and includes the frames that led to the
    // SIGSEGV.
    //
    // [0]: https://man7.org/linux/man-pages/man7/signal-safety.7.html

    static SEEN: AtomicUsize = AtomicUsize::new(0);
    match SEEN.fetch_add(1, Ordering::SeqCst) {
        0 => {
            // First SIGSEGV. See if we can defer to our slick panic handler,
            // which will emit a backtrace and details on where to submit bugs.
            panic!("received SIGSEGV or SIGBUS (maybe a stack overflow?)");
        }
        _ => {
            // Second SIGSEGV, which means the panic handler itself segfaulted.
            // This usually indicates that the memory allocator state is
            // corrupt, which can happen if we overflow the stack while inside
            // the allocator. Just try to eke out a message and crash.
            let _ = io::stderr().write_all(b"SIGBUS or SIGSEGV while handling SIGSEGV or SIGBUS\n");
            let _ = io::stderr().write_all(b"(maybe a stack overflow while allocating?)\n");
            process::abort();
        }
    }
}
