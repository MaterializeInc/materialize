// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! System support functions.

use log::{trace, warn};

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
