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

//! Physical memory introspection.

// Only this probe uses the cgroup helpers, so the module lives here rather
// than at the crate root. The file carries more surface than the probe
// needs, hence the dead-code allowance.
#[cfg(target_os = "linux")]
#[path = "cgroup.rs"]
#[allow(dead_code)]
mod cgroup;

/// Returns the physical memory available to this process in bytes: the
/// host's RAM, clamped by the cgroup memory limit when one is set. Both
/// cgroup v1 and v2 are honored, resolved through `/proc/self/mountinfo`
/// and `/proc/self/cgroup` rather than an assumed mount path, so containers
/// (including host-namespace cgroup mounts) do not derive budgets from host
/// RAM. `None` if detection fails.
///
/// Deliberately distinct from any *announced* memory limit: on nodes whose
/// disk is provisioned as swap, the announced limit includes swap so the
/// memory limiter can bound total heap. Budgets that bound *resident* bytes
/// must instead derive from memory that can be resident, which is what this
/// reports.
pub fn physical_memory_bytes() -> Option<usize> {
    let host = host_memory_bytes()?;
    match cgroup_memory_max() {
        Some(limit) if limit < host => Some(limit),
        _ => Some(host),
    }
}

#[cfg(target_os = "linux")]
fn host_memory_bytes() -> Option<usize> {
    let meminfo = std::fs::read_to_string("/proc/meminfo").ok()?;
    let line = meminfo.lines().find(|l| l.starts_with("MemTotal:"))?;
    let kib: usize = line.split_whitespace().nth(1)?.parse().ok()?;
    Some(kib * 1024)
}

#[cfg(target_os = "macos")]
fn host_memory_bytes() -> Option<usize> {
    let mut size: u64 = 0;
    let mut len = std::mem::size_of::<u64>();
    // SAFETY: `sysctlbyname` reads into an out-buffer of the size we report;
    // `hw.memsize` is a `u64` and `len` matches.
    let ret = unsafe {
        libc::sysctlbyname(
            c"hw.memsize".as_ptr(),
            std::ptr::from_mut(&mut size).cast::<libc::c_void>(),
            &mut len,
            std::ptr::null_mut(),
            0,
        )
    };
    if ret == 0 {
        usize::try_from(size).ok()
    } else {
        None
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn host_memory_bytes() -> Option<usize> {
    None
}

/// The RAM limit of the cgroup governing this process, if any.
#[cfg(target_os = "linux")]
fn cgroup_memory_max() -> Option<usize> {
    cgroup::detect_memory_limit()?.max
}

#[cfg(not(target_os = "linux"))]
fn cgroup_memory_max() -> Option<usize> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn detects_some_memory() {
        let bytes = physical_memory_bytes().expect("detection works on test platforms");
        // Sanity: more than 64 MiB, less than 1 PiB.
        assert!(bytes > 64 << 20);
        assert!(bytes < 1 << 50);
    }
}
