// Copyright 2014 The Rust Project Developers
// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.
//
// Portions of this file are derived from the stdlog component of the slog
// project. The original source code was retrieved on March 14, 2019 from:
//
//     https://github.com/slog-rs/stdlog/blob/master/lib.rs
//
// The original source code is subject to the terms of the MIT license, a copy
// of which can be found in the LICENSE file at the root of this repository.

//! Logging utilities.

use slog::o;
use slog::{Drain, Serializer, KV};
use std::fmt;
use std::io;
use std::io::Write;
use std::sync::Once;

struct StdLog;

struct LazyLogString<'a> {
    info: &'a slog::Record<'a>,
    logger_values: &'a slog::OwnedKVList,
}

impl<'a> LazyLogString<'a> {
    fn new(info: &'a slog::Record, logger_values: &'a slog::OwnedKVList) -> Self {
        LazyLogString {
            info,
            logger_values,
        }
    }
}

impl<'a> fmt::Display for LazyLogString<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.info.msg())?;

        let io = io::Cursor::new(Vec::new());
        let mut ser = Ksv::new(io);

        let res = {
            || -> io::Result<()> {
                self.logger_values.serialize(self.info, &mut ser)?;
                self.info.kv().serialize(self.info, &mut ser)?;
                Ok(())
            }
        }()
        .map_err(|_| fmt::Error);

        res?;

        let values = ser.into_inner().into_inner();

        write!(f, "{}", String::from_utf8_lossy(&values))
    }
}

/// A key-separator-value serializer.
struct Ksv<W: io::Write> {
    io: W,
}

impl<W: io::Write> Ksv<W> {
    fn new(io: W) -> Self {
        Ksv { io }
    }

    fn into_inner(self) -> W {
        self.io
    }
}

impl<W: io::Write> Serializer for Ksv<W> {
    fn emit_arguments(&mut self, key: slog::Key, val: &fmt::Arguments) -> slog::Result {
        write!(self.io, ", {}: {}", key, val)?;
        Ok(())
    }
}

impl Drain for StdLog {
    type Err = io::Error;
    type Ok = ();
    fn log(&self, info: &slog::Record, logger_values: &slog::OwnedKVList) -> io::Result<()> {
        let level = match info.level() {
            slog::Level::Critical | slog::Level::Error => log::Level::Error,
            slog::Level::Warning => log::Level::Warn,
            slog::Level::Info => log::Level::Info,
            slog::Level::Debug => log::Level::Debug,
            slog::Level::Trace => log::Level::Trace,
        };
        let target = if info.tag().is_empty() {
            info.module()
        } else {
            info.tag()
        };
        let location = (target, info.module(), info.file(), info.line());
        let lazy = LazyLogString::new(info, logger_values);
        log::__private_api_log(format_args!("{}", lazy), level, &location);
        Ok(())
    }
}

/// Create a [`slog::Logger`] that simply proxies to the [`log`] crate.
pub fn slog_adapter() -> slog::Logger {
    let drain = StdLog.fuse();
    slog::Logger::root(drain, o!())
}

static LOG_INIT: Once = Once::new();

/// Initialize global logger, using the [`log`] crate, with sensible defaults.
///
/// It is safe to call `init` multiple times. This is mostly for the convenience
/// of tests, which are not run in any particular order, and therefore must each
/// call `init`.
pub fn init() {
    LOG_INIT.call_once(|| {
        env_logger::Builder::from_env(env_logger::Env::new().filter_or("MTRLZ_LOG", "info"))
            .format(|buf, record| {
                let ts = buf.timestamp_nanos();
                let level = buf.default_styled_level(record.level());
                write!(buf, "[{} {} ", ts, level)?;
                match (record.file(), record.line()) {
                    (Some(file), Some(line)) => {
                        let search = "/.cargo/";
                        let file = match file.find(search) {
                            Some(index) => &file[search.len() + index..],
                            None => file,
                        }
                        .trim_start_matches("registry/src/")
                        .trim_start_matches("git/checkouts/");
                        write!(buf, "{}:{}", file, line)?;
                    }
                    _ => write!(buf, "(unknown)")?,
                };
                writeln!(buf, "] {}", record.args())
            })
            .init();
    });
}
