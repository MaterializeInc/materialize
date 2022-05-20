// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! System support functions.

use anyhow::Context;
use nix::errno;
use nix::sys::signal;
use tracing::trace;

#[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "ios")))]
pub fn adjust_rlimits() {
    trace!("rlimit crate does not support this OS; not adjusting nofile limit");
}

/// Attempts to increase the soft nofile rlimit to the maximum possible value.
#[cfg(any(target_os = "macos", target_os = "linux", target_os = "ios"))]
pub fn adjust_rlimits() {
    use rlimit::Resource;
    use tracing::warn;

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
        use mz_ore::result::ResultExt;
        use std::cmp;
        use sysctl::Sysctl;

        // On macOS, getrlimit by default reports that the hard limit is
        // unlimited, but there is usually a stricter hard limit discoverable
        // via sysctl. Failing to discover this secret stricter hard limit will
        // cause the call to setrlimit below to fail.
        let res = sysctl::Ctl::new("kern.maxfilesperproc")
            .and_then(|ctl| ctl.value())
            .map_err_to_string()
            .and_then(|v| match v {
                sysctl::CtlValue::Int(v) => u64::try_from(v)
                    .map_err(|_| format!("kern.maxfilesperproc unexpectedly negative: {}", v)),
                o => Err(format!("unexpected sysctl value type: {:?}", o)),
            });
        match res {
            Ok(v) => {
                trace!("sysctl kern.maxfilesperproc hard limit: {}", v);
                cmp::min(v, hard)
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

    const RECOMMENDED_SOFT: u64 = 1024;
    if soft < RECOMMENDED_SOFT {
        warn!(
            "soft nofile rlimit ({}) is dangerously low; at least {} is recommended",
            soft, RECOMMENDED_SOFT
        )
    }
}

pub fn enable_sigusr2_coverage_dump() -> Result<(), anyhow::Error> {
    let action = signal::SigAction::new(
        signal::SigHandler::Handler(handle_sigusr2_signal),
        signal::SaFlags::SA_NODEFER | signal::SaFlags::SA_ONSTACK,
        signal::SigSet::empty(),
    );

    unsafe { signal::sigaction(signal::SIGUSR2, &action) }
        .context("failed to install SIGUSR2 handler")?;

    Ok(())
}

pub fn enable_termination_signal_cleanup() -> Result<(), anyhow::Error> {
    let action = signal::SigAction::new(
        signal::SigHandler::Handler(handle_termination_signal),
        signal::SaFlags::SA_NODEFER | signal::SaFlags::SA_ONSTACK,
        signal::SigSet::empty(),
    );

    for signum in &[
        signal::SIGHUP,
        signal::SIGINT,
        signal::SIGALRM,
        signal::SIGTERM,
        signal::SIGUSR1,
    ] {
        unsafe { signal::sigaction(*signum, &action) }
            .with_context(|| format!("failed to install handler for {}", signum))?;
    }

    Ok(())
}

extern "C" {
    fn __llvm_profile_write_file() -> libc::c_int;
}

extern "C" fn handle_sigusr2_signal(_: i32) {
    let _ = unsafe { __llvm_profile_write_file() };
}

extern "C" fn handle_termination_signal(signum: i32) {
    let _ = unsafe { __llvm_profile_write_file() };

    let action = signal::SigAction::new(
        signal::SigHandler::SigDfl,
        signal::SaFlags::SA_NODEFER | signal::SaFlags::SA_ONSTACK,
        signal::SigSet::empty(),
    );
    unsafe { signal::sigaction(signum.try_into().unwrap(), &action) }
        .unwrap_or_else(|_| panic!("failed to uninstall handler for {}", signum));

    let ret = unsafe { libc::raise(signum) };
    if ret == -1 {
        let errno = errno::from_i32(errno::errno());
        panic!("failed to re-raise signal {}: {}", signum, errno);
    }
}
