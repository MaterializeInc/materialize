// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Error handling.
//!
//! Errors inside of the testdrive library are represented as an
//! `anyhow::Error`. As the error bubbles up the stack, it may be upgraded to a
//! `PosError`, which attaches source code position information. The position
//! information tracked by a `PosError` uses a parser-specific representation
//! that is not human-readable, so `PosError`s are upgraded to `Error`s before
//! they are returned externally.

use std::fmt::Write as _;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use atty::Stream;
use termcolor::{Color, ColorChoice, ColorSpec, StandardStream, WriteColor};

/// An error produced when parsing or executing a testdrive script.
///
/// Errors are optionally associated with a location in a testdrive script. When
/// printed with the [`Error::print_stderr`] method, the location of the error
/// along with a snippet of the source code at that location will be printed
/// alongside the error message.
pub struct Error {
    source: anyhow::Error,
    location: Option<ErrorLocation>,
}

impl Error {
    pub(crate) fn new(source: anyhow::Error, location: Option<ErrorLocation>) -> Self {
        Error { source, location }
    }

    /// Prints the error to `stderr`, with coloring if the terminal supports it.
    pub fn print_stderr(&self) -> io::Result<()> {
        let color_choice = if atty::is(Stream::Stderr) {
            ColorChoice::Auto
        } else {
            ColorChoice::Never
        };
        let mut stderr = StandardStream::stderr(color_choice);
        println!("^^^ +++");
        match &self.location {
            Some(location) => {
                let mut color_spec = ColorSpec::new();
                color_spec.set_bold(true);
                stderr.set_color(&color_spec)?;
                write!(
                    &mut stderr,
                    "{}:{}:{}: ",
                    location.filename.display(),
                    location.line,
                    location.col
                )?;
                write_error_heading(&mut stderr, &color_spec)?;
                writeln!(&mut stderr, "{:#}", self.source)?;
                color_spec.set_bold(false);
                stderr.set_color(&color_spec)?;
                write!(&mut stderr, "{}", location.snippet)?;
                writeln!(&mut stderr, "{}^", " ".repeat(location.col - 1))
            }
            None => {
                let color_spec = ColorSpec::new();
                write_error_heading(&mut stderr, &color_spec)?;
                writeln!(&mut stderr, "{:#}", self.source)?;
                Ok(())
            }
        }
    }
}

fn write_error_heading(stream: &mut StandardStream, color_spec: &ColorSpec) -> io::Result<()> {
    stream.set_color(color_spec.clone().set_fg(Some(Color::Red)))?;
    write!(stream, "error: ")?;
    stream.set_color(color_spec)
}

impl From<anyhow::Error> for Error {
    fn from(source: anyhow::Error) -> Error {
        Error {
            source,
            location: None,
        }
    }
}

pub(crate) struct ErrorLocation {
    filename: PathBuf,
    snippet: String,
    line: usize,
    col: usize,
}

impl ErrorLocation {
    pub(crate) fn new(filename: &Path, contents: &str, line: usize, col: usize) -> ErrorLocation {
        let mut snippet = String::new();
        writeln!(&mut snippet, "     |").unwrap();
        for (i, l) in contents.lines().enumerate() {
            if i >= line {
                break;
            } else if i + 2 >= line {
                writeln!(&mut snippet, "{:4} | {}", i + 1, l).unwrap();
            }
        }
        write!(&mut snippet, "     | ").unwrap();

        ErrorLocation {
            filename: filename.to_path_buf(),
            snippet,
            line,
            col,
        }
    }
}

pub(crate) struct PosError {
    pub(crate) source: anyhow::Error,
    pub(crate) pos: Option<usize>,
}

impl PosError {
    pub(crate) fn new(source: anyhow::Error, pos: usize) -> PosError {
        PosError {
            source,
            pos: Some(pos),
        }
    }
}

impl From<anyhow::Error> for PosError {
    fn from(source: anyhow::Error) -> PosError {
        PosError { source, pos: None }
    }
}
