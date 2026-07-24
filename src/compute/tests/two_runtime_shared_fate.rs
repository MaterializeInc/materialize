// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Verifies the two-runtime shared-fate failure model: a panic on any worker or reader thread of
//! either the maintenance or the interactive compute runtime aborts the whole process, so an
//! interactive import's read hold cannot outlive the replica (see `clusterd/src/lib.rs`, the
//! interactive `serve` call site, for the design rationale).
//!
//! This is a subprocess test rather than an in-process one: the property under test is that the
//! process *aborts* (SIGABRT), which cannot be observed from inside the panicking process itself.
//! The child installs `mz_ore::panic::install_enhanced_handler` exactly as `clusterd::main` does,
//! before spawning two thread-based stand-ins for the maintenance and interactive runtimes' worker
//! threads (bare `timely::execute_directly` clusters, matching the two-runtime shape used in
//! `mz_compute::sharing`'s tests), then panics on the interactive one. This stays hermetic (no
//! persist/network setup) while exercising the exact hook wiring `clusterd::main` uses; the full
//! `serve` stack is proven to boot both runtimes by task 2c's tests.

use std::process::Command;
use std::time::Duration;

/// Set in the child invocation to select the panicking behavior instead of the harness behavior.
const CHILD_ENV_VAR: &str = "MZ_TWO_RUNTIME_SHARED_FATE_CHILD";

#[cfg(unix)]
#[mz_ore::test]
fn interactive_worker_panic_aborts_process() {
    use std::os::unix::process::ExitStatusExt;

    if std::env::var_os(CHILD_ENV_VAR).is_some() {
        run_child_and_exit();
    }

    let exe = std::env::current_exe().expect("test binary has a path");
    let output = Command::new(exe)
        .args([
            "--exact",
            "interactive_worker_panic_aborts_process",
            "--nocapture",
        ])
        .env(CHILD_ENV_VAR, "1")
        .output()
        .expect("spawn child test process");

    // `process::abort()` (the enhanced panic handler's terminal action) raises SIGABRT on unix,
    // distinct from a normal nonzero exit. If the panic were instead swallowed or downgraded to a
    // normal error return, the child would reach `run_child_and_exit`'s failure path below and
    // exit normally with code 1, which fails this assertion with the captured output attached.
    assert_eq!(
        output.status.signal(),
        Some(libc::SIGABRT),
        "expected the child to abort via SIGABRT; got {:?}\n--- child stdout ---\n{}\n--- child stderr ---\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}

/// Runs the actual two-runtime-in-one-process panic scenario and terminates the child process.
/// Never returns: either the process aborts on the interactive worker's panic (the property under
/// test), or the scenario fails to reproduce the abort and this exits normally with code 1.
fn run_child_and_exit() -> ! {
    // Mirrors `clusterd::main`, which installs this before either `serve` call, covering both
    // runtimes' worker and reader threads with one process-global hook.
    mz_ore::panic::install_enhanced_handler();

    // Stand-in for the maintenance runtime: a worker thread that stays alive so the panic below
    // has to take down a live sibling runtime's thread too, not just an empty process.
    let _maintenance = std::thread::spawn(|| {
        timely::execute_directly(|_worker| {
            std::thread::sleep(Duration::from_secs(30));
        });
    });
    std::thread::sleep(Duration::from_millis(100));

    // Stand-in for the interactive runtime: panics on its (only) worker thread.
    let interactive = std::thread::spawn(|| {
        timely::execute_directly(|_worker| {
            panic!("simulated interactive-runtime worker panic");
        });
    });

    // The enhanced panic handler aborts the whole process from within the panicking thread, so
    // this join is not expected to return. Reaching past it means the panic did not bring the
    // process down, i.e. the shared-fate property this test guards does not hold.
    let _ = interactive.join();
    eprintln!("FAIL: process survived an interactive-runtime worker panic instead of aborting");
    std::process::exit(1);
}
