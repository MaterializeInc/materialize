// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.
//

//! The main Materialize server.
//!
//! The name is pronounced "materialize-dee." It listens on port 6875 (MTRL).
//!
//! The design and implementation of materialized is very much in flux. See the
//! draft architecture doc for the most up-to-date plan [0]. Access is limited
//! to those with access to the Material Dropbox Paper folder.
//!
//! [0]: https://paper.dropbox.com/doc/Materialize-architecture-plans--AYSu6vvUu7ZDoOEZl7DNi8UQAg-sZj5rhJmISdZSfK0WBxAl

use backtrace::Backtrace;
// use getopts::Options;
use lazy_static::lazy_static;
// use std::env;
use std::error::Error;
use std::panic;
use std::panic::PanicInfo;
use std::process;
use std::sync::Mutex;
use std::thread;

fn main() -> Result<(), Box<dyn Error>> {
    panic::set_hook(Box::new(handle_panic));
    ore::log::init();

    let timely_configuration = timely::Configuration::from_args(std::env::args())?;
    let materialize_configuration = materialize::server::Config::from_timely(timely_configuration);
    materialize::server::serve(materialize_configuration)?;
    Ok(())
}

lazy_static! {
    static ref PANIC_MUTEX: Mutex<()> = Mutex::new(());
}

fn handle_panic(panic_info: &PanicInfo) {
    let _guard = PANIC_MUTEX.lock();

    let thr = thread::current();
    let thr_name = thr.name().unwrap_or("<unnamed>");

    let msg = match panic_info.payload().downcast_ref::<&'static str>() {
        Some(s) => *s,
        None => match panic_info.payload().downcast_ref::<String>() {
            Some(s) => &s[..],
            None => "Box<Any>",
        },
    };

    let backtrace = Backtrace::new();

    eprintln!(
        r#"materialized encountered an internal error and crashed.

We rely on bug reports to diagnose and fix these errors. Please
copy and paste the following details and mail them to bugs@materialize.io.
To protect your privacy, we do not collect crash reports automatically.

 thread: {}
message: {}
{:?}
"#,
        thr_name, msg, backtrace
    );

    process::exit(1);
}
