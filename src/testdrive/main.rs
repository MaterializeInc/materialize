// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! A driver for Materialize integration tests.

use testdrive::run;

use std::process;

fn main() {
    if let Err(err) = run() {
        // If printing the error message fails, there's not a whole lot we can
        // do.
        let _ = err.print_stderr();
        process::exit(1);
    }
}
