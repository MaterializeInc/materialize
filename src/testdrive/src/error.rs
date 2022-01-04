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
//! Testdrive takes pains to provide better error messages than are typical of a
//! Rust program. The main error type, [`Error`], is not intentionally not
//! constructible from from underlying errors without providing additional
//! context. The [`ResultExt`] trait can be used to ergonomically add this
//! context to the error within the result. For example, here is an idiomatic
//! example of handling a filesystem error:
//!
//! ```ignore
//! use std::fs::File;
//! use std::io::Read;
//! use testdrive::error::{Error, ResultExt};
//!
//! fn check_file(path: &str) -> Result<bool, Error> {
//!     let mut file = File::open(&path).err_ctx(format!("opening {}", path))?;
//!
//!     let mut contents = String::new();
//!     file.read_to_string(&mut contents).err_ctx(format!("reading {}", path))?;
//!     Ok(contents.contains("AOK"))
//! }
//! ```

use std::error::Error as StdError;
use std::fmt::{self, Write as FmtWrite};
use std::io::{self, Write};
use std::iter::IntoIterator;

use atty::Stream;
use termcolor::{Color, ColorChoice, ColorSpec, StandardStream, WriteColor};

pub type DynError = Box<dyn StdError + Send + Sync>;

/// A testdrive error.
///
#[derive(Debug)]
pub enum Error {
    /// An error in parsing a testdrive script.
    Input {
        /// The underlying error.
        err: InputError,
        /// Additional details.
        details: Option<InputDetails>,
    },
    /// Other errors.
    General {
        /// The component in which the error occurred.
        ctx: String,
        /// The underlying causes of the error.
        causes: Vec<DynError>,
        /// Hints about how to resolve the error.
        hints: Vec<String>,
    },
}

impl Error {
    /// Create a new error with just the specified message
    pub(crate) fn message(m: impl Into<String>) -> Error {
        Error::General {
            ctx: m.into(),
            causes: Vec::new(),
            hints: Vec::new(),
        }
    }

    /// Create a new error with the specified message and cause
    pub fn with_cause(m: impl Into<String>, cause: impl Into<DynError>) -> Error {
        Error::General {
            ctx: m.into(),
            causes: vec![cause.into()],
            hints: Vec::new(),
        }
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
        match self {
            Error::Input {
                err,
                details: Some(details),
            } => {
                let mut color_spec = ColorSpec::new();
                color_spec.set_bold(true);
                stderr.set_color(&color_spec)?;
                write!(
                    &mut stderr,
                    "{}:{}:{}: ",
                    details.filename, details.line, details.col
                )?;
                write_error_heading(&mut stderr, &color_spec)?;
                writeln!(&mut stderr, "{}", err.msg)?;
                color_spec.set_bold(false);
                stderr.set_color(&color_spec)?;
                write!(&mut stderr, "{}", details.snippet)?;
                writeln!(&mut stderr, "{}^", " ".repeat(details.col - 1))
            }
            Error::Input { details: None, .. } => {
                panic!("programming error: print_stderr called on InputError with no details")
            }
            Error::General { ctx, causes, hints } => {
                let color_spec = ColorSpec::new();
                write_error_heading(&mut stderr, &color_spec)?;
                write!(&mut stderr, "{}", ctx)?;
                for (i, cause) in causes.iter().enumerate() {
                    if i == 0 {
                        write!(&mut stderr, ": {}", cause)?;
                    } else {
                        write!(&mut stderr, ", {}", cause)?;
                    }
                }
                writeln!(&mut stderr)?;

                for hint in hints {
                    stderr.set_color(&color_spec.clone().set_bold(true))?;
                    write!(&mut stderr, " hint: ")?;
                    stderr.set_color(&color_spec)?;
                    writeln!(&mut stderr, "{}", hint)?;
                }
                Ok(())
            }
        }
    }

    pub(crate) fn with_input_details(
        self,
        filename: &str,
        contents: &str,
        positioner: &dyn Positioner,
    ) -> Self {
        match self {
            Error::Input { err, .. } => {
                let (line, col) = positioner.line_col(err.pos);
                let details = InputDetails {
                    filename: filename.to_owned(),
                    snippet: make_snippet(contents, line),
                    line,
                    col,
                };
                Error::Input {
                    err,
                    details: Some(details),
                }
            }
            _ => self,
        }
    }
}

impl From<InputError> for Error {
    fn from(err: InputError) -> Error {
        Error::Input { err, details: None }
    }
}

fn make_snippet(contents: &str, line_num: usize) -> String {
    let mut buf = String::new();
    writeln!(&mut buf, "     |").unwrap();
    for (i, line) in contents.lines().enumerate() {
        if i >= line_num {
            break;
        } else if i + 2 >= line_num {
            writeln!(&mut buf, "{:4} | {}", i + 1, line).unwrap();
        }
    }
    write!(&mut buf, "     | ").unwrap();
    buf
}

fn write_error_heading(stream: &mut StandardStream, color_spec: &ColorSpec) -> io::Result<()> {
    stream.set_color(color_spec.clone().set_fg(Some(Color::Red)))?;
    write!(stream, "error: ")?;
    stream.set_color(color_spec)
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::Input { err, .. } => write!(f, "{}", err.msg),
            Error::General { ctx, causes, .. } => {
                write!(f, "{}", ctx)?;
                write!(f, "{}", ctx)?;
                for (i, cause) in causes.iter().enumerate() {
                    if i == 0 {
                        write!(f, ": {}", cause)?;
                    } else {
                        write!(f, ", {}", cause)?;
                    }
                    writeln!(f)?;
                }
                Ok(())
            }
        }
    }
}

impl std::error::Error for Error {}

#[derive(Debug)]
pub struct InputError {
    pub msg: String,
    pub pos: usize,
}

#[derive(Debug)]
pub struct InputDetails {
    filename: String,
    snippet: String,
    line: usize,
    col: usize,
}

impl InputDetails {
    pub fn filename(&self) -> String {
        return self.filename.clone();
    }
}

pub trait Positioner {
    fn line_col(&self, pos: usize) -> (usize, usize);
}

/// Extra methods that integrate std Results with [`Error`]
pub trait ResultExt<T, E> {
    /// Wrap any error in an [`Error`] with the given context message
    fn err_ctx(self, ctx: impl Into<String>) -> Result<T, Error>
    where
        Self: Sized;

    /// Wrap any error in an [`Error`] with the given context message, which is lazily evaluated
    fn with_err_ctx(self, ctx: impl FnOnce() -> String) -> Result<T, Error>
    where
        Self: Sized;

    /// Wrap any error in an [`Error`] with the given context message and additional hints
    fn err_hint(
        self,
        ctx: impl Into<String>,
        hint: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<T, Error>
    where
        Self: Sized;
}

impl<T, E> ResultExt<T, E> for Result<T, E>
where
    E: 'static + Send + Sync,
    E: Into<DynError>,
{
    fn err_ctx(self, ctx: impl Into<String>) -> Result<T, Error>
    where
        Self: Sized,
    {
        self.map_err(|err| Error::General {
            ctx: ctx.into(),
            causes: vec![err.into()],
            hints: Vec::new(),
        })
    }

    fn with_err_ctx(self, ctx: impl FnOnce() -> String) -> Result<T, Error>
    where
        Self: Sized,
    {
        self.map_err(|err| Error::General {
            ctx: ctx(),
            causes: vec![err.into()],
            hints: Vec::new(),
        })
    }

    fn err_hint(
        self,
        ctx: impl Into<String>,
        hints: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<T, Error>
    where
        Self: Sized,
    {
        self.map_err(|err| Error::General {
            ctx: ctx.into(),
            causes: vec![err.into()],
            hints: hints.into_iter().map(Into::into).collect(),
        })
    }
}
