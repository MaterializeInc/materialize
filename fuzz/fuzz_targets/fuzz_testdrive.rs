// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

#![no_main]
#[macro_use]
extern crate libfuzzer_sys;

fuzz_target!(|data: &[u8]| {
    if let Ok(string) = std::str::from_utf8(data) {
        match testdrive::run_string("<fuzzer>", &string) {
            Ok(()) | Err(testdrive::Error::Input { .. }) | Err(testdrive::Error::Usage) => (),
            Err(error @ testdrive::Error::General { .. }) => panic!("{}", error),
        }
    };
});
