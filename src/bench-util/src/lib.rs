// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Criterion that counts instructions when `MZ_BENCH_MEASUREMENT=instructions`.
//!
//! Benches import from this crate instead of `criterion` and measure wall time
//! by default, or the user-space instructions the benchmarking thread retires.
//! Instruction counts miss other threads and the kernel, so benches that spawn
//! threads or do I/O stay on plain criterion.

use std::fs::File;
use std::io::Read;
use std::time::{Duration, Instant};

use criterion::measurement::{ValueFormatter, WallTime};
pub use criterion::*;

pub type Criterion = criterion::Criterion<Measurement>;
pub type Bencher<'a> = criterion::Bencher<'a, Measurement>;

#[macro_export]
macro_rules! criterion_group {
    ($name:ident, $($target:path),+ $(,)?) => {
        $crate::criterion::criterion_group! {
            name = $name;
            config = $crate::criterion::Criterion::default()
                .with_measurement($crate::Measurement::from_env());
            targets = $($target),+
        }
    };
}

#[doc(hidden)]
pub use criterion;

static WALL: WallTime = WallTime;

/// Wall time, or with a counter, instruction counts passed off as nanoseconds
/// so that criterion's wall-time arithmetic applies unchanged.
pub struct Measurement(Option<File>);

impl Measurement {
    /// Reads `MZ_BENCH_MEASUREMENT`, panicking unless it is unset, `wall`, or
    /// `instructions` on a host that can count them.
    pub fn from_env() -> Self {
        Measurement(match std::env::var("MZ_BENCH_MEASUREMENT").as_deref() {
            Err(_) | Ok("wall") => None,
            Ok("instructions") => Some(open_instruction_counter()),
            Ok(other) => panic!("MZ_BENCH_MEASUREMENT must be wall or instructions, not {other}"),
        })
    }
}

impl criterion::measurement::Measurement for Measurement {
    type Intermediate = (Instant, u64);
    type Value = Duration;

    fn start(&self) -> (Instant, u64) {
        (Instant::now(), self.0.as_ref().map_or(0, read))
    }

    fn end(&self, (start, count): (Instant, u64)) -> Duration {
        match &self.0 {
            None => start.elapsed(),
            Some(counter) => Duration::from_nanos(read(counter) - count),
        }
    }

    fn add(&self, v1: &Duration, v2: &Duration) -> Duration {
        *v1 + *v2
    }

    fn zero(&self) -> Duration {
        Duration::ZERO
    }

    fn to_f64(&self, value: &Duration) -> f64 {
        WALL.to_f64(value)
    }

    fn formatter(&self) -> &dyn ValueFormatter {
        match self.0 {
            None => WALL.formatter(),
            Some(_) => &Instructions,
        }
    }
}

struct Instructions;

impl ValueFormatter for Instructions {
    fn scale_values(&self, _: f64, _: &mut [f64]) -> &'static str {
        "instructions"
    }

    fn scale_throughputs(&self, _: f64, _: &Throughput, _: &mut [f64]) -> &'static str {
        "instructions"
    }

    fn scale_for_machines(&self, _: &mut [f64]) -> &'static str {
        "instructions"
    }
}

#[cfg(target_os = "linux")]
fn open_instruction_counter() -> File {
    use std::os::fd::FromRawFd;

    // `struct perf_event_attr`, which libc lacks: `type` (0, PERF_TYPE_HARDWARE)
    // and `size` in the first word, `config` in the second, flags in the sixth.
    let mut attr = [0u64; 16];
    attr[0] = 128 << 32;
    attr[1] = 1; // PERF_COUNT_HW_INSTRUCTIONS
    attr[5] = (1 << 5) | (1 << 6); // exclude_kernel, exclude_hv
    // SAFETY: `attr` is a `perf_event_attr` of the size it declares. pid 0 and
    // cpu -1 count the calling thread on any CPU.
    let fd = unsafe { libc::syscall(libc::SYS_perf_event_open, attr.as_ptr(), 0, -1, -1, 0) };
    assert!(
        fd >= 0,
        "perf_event_open: {}. It needs kernel.perf_event_paranoid <= 2, and CAP_PERFMON in Docker.",
        std::io::Error::last_os_error()
    );
    // SAFETY: the kernel just returned `fd`, and nothing else owns it.
    unsafe { File::from_raw_fd(i32::try_from(fd).unwrap()) }
}

#[cfg(not(target_os = "linux"))]
fn open_instruction_counter() -> File {
    panic!("counting instructions needs Linux perf events")
}

fn read(mut counter: &File) -> u64 {
    let mut buf = [0; 8];
    counter.read_exact(&mut buf).expect("reading the counter");
    u64::from_ne_bytes(buf)
}
