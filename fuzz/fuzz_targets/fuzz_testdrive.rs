// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

#![cfg_attr(not(test), no_main)]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    if let Ok(string) = std::str::from_utf8(data) {
        match testdrive::run_string(&testdrive::Config::default(), "<fuzzer>", &string) {
            Ok(())
            | Err(testdrive::Error::Input { .. })
            | Err(testdrive::Error::Usage { .. })
            | Err(testdrive::Error::Message { .. }) => {}
            Err(error @ testdrive::Error::General { .. }) => panic!("{}", error),
        }
    };
});
