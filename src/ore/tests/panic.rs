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

use std::panic;

use mz_ore::panic::{catch_unwind_str, catch_unwind_with_details, install_enhanced_handler};
use scopeguard::defer;

// IMPORTANT!!! Do not add any additional tests to this file. This test sets and
// removes panic hooks and can interfere with any concurrently running test.
// Therefore, it needs to be run in isolation.

#[test] // allow(test-attribute)
fn catch_panic() {
    let old_hook = panic::take_hook();
    defer! {
        panic::set_hook(old_hook);
    }

    install_enhanced_handler();

    let result = catch_unwind_str(|| {
        panic!("panicked");
    })
    .unwrap_err();

    assert_eq!(result, "panicked");

    // `catch_unwind_with_details` additionally recovers the panic location and a
    // backtrace captured at the panic site.
    let line = line!() + 2;
    let caught = catch_unwind_with_details(|| {
        panic!("panicked with details");
    })
    .unwrap_err();

    assert_eq!(caught.message, "panicked with details");

    let location = caught
        .location
        .as_deref()
        .expect("location should be captured");
    assert!(
        location.contains("tests/panic.rs"),
        "unexpected location: {location}"
    );
    assert!(
        location.contains(&format!(":{line}:")),
        "location {location} should reference line {line}"
    );

    let backtrace = caught
        .backtrace
        .as_deref()
        .expect("backtrace should be captured");
    assert!(!backtrace.is_empty());

    // The `Display` impl appends the location to the message.
    assert_eq!(
        caught.to_string(),
        format!("panicked with details, at {location}")
    );
}
