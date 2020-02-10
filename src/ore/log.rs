// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Logging utilities.

use std::io::Write;
use std::sync::Once;

static LOG_INIT: Once = Once::new();

/// Initialize global logger, using the [`log`] crate, with sensible defaults.
///
/// It is safe to call `init` multiple times. This is mostly for the convenience
/// of tests, which are not run in any particular order, and therefore must each
/// call `init`.
pub fn init() {
    LOG_INIT.call_once(|| {
        env_logger::Builder::from_env(env_logger::Env::new().filter_or("MZ_LOG", "info"))
            .format(|buf, record| {
                let ts = buf.timestamp_micros();
                let level = buf.default_styled_level(record.level());
                write!(buf, "[{} {:>5} ", ts, level)?;
                match (record.file(), record.line()) {
                    (Some(file), Some(line)) => {
                        let search = "/.cargo/";
                        let file = match file.find(search) {
                            Some(index) => &file[search.len() + index..],
                            None => file,
                        }
                        .trim_start_matches("registry/src/")
                        .trim_start_matches("git/checkouts/")
                        .trim_start_matches("src/")
                        .trim_end_matches(".rs");
                        write!(buf, "{}:{}", file, line)?;
                    }
                    _ => write!(buf, "(unknown)")?,
                };
                writeln!(buf, "] {}", record.args())
            })
            .init();
    });
}
