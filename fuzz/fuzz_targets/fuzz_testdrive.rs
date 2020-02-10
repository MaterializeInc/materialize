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

use testdrive::error::Error;

fuzz_target!(|data: &[u8]| {
    if let Ok(string) = std::str::from_utf8(data) {
        match testdrive::run_string(&testdrive::Config::default(), "<fuzzer>", &string) {
            Ok(()) | Err(Error::Input { .. }) | Err(Error::Usage { .. }) => {}
            Err(error @ Error::General { .. }) => panic!("{}", error),
        }
    };
});
