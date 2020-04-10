// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![cfg_attr(not(test), no_main)]

use libfuzzer_sys::fuzz_target;
use tokio::runtime::Runtime;

use testdrive::error::Error;
use testdrive::Config;

fuzz_target!(|data: &[u8]| {
    let mut runtime = Runtime::new().unwrap();
    if let Ok(string) = std::str::from_utf8(data) {
        match runtime.block_on(testdrive::run_string(
            &Config::default(),
            "<fuzzer>",
            &string,
        )) {
            Ok(()) | Err(Error::Input { .. }) | Err(Error::Usage { .. }) => {}
            Err(error @ Error::General { .. }) => panic!("{}", error),
        }
    };
});
