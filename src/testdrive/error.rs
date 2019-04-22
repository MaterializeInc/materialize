// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use atty::Stream;
use std::error::Error as StdError;
use std::fmt;
use std::fmt::Write as FmtWrite;
use std::io;
use std::io::Write;
use termcolor::{Color, ColorChoice, ColorSpec, StandardStream, WriteColor};

#[derive(Debug)]
pub enum Error {
    Input {
        err: InputError,
        details: Option<InputDetails>,
    },
    General {
        ctx: String,
        cause: Box<dyn StdError>,
        hints: Vec<String>,
    },
    Usage {
        details: String,
        requested: bool,
    },
}

impl Error {
    pub fn print_stderr(&self) -> io::Result<()> {
        let color_choice = if atty::is(Stream::Stderr) {
            ColorChoice::Auto
        } else {
            ColorChoice::Never
        };
        let mut stderr = StandardStream::stderr(color_choice);
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
            Error::General { ctx, cause, hints } => {
                let color_spec = ColorSpec::new();
                write_error_heading(&mut stderr, &color_spec)?;
                writeln!(&mut stderr, "{}: {}", ctx, cause)?;
                for hint in hints {
                    stderr.set_color(&color_spec.clone().set_bold(true))?;
                    write!(&mut stderr, " hint: ")?;
                    stderr.set_color(&color_spec)?;
                    writeln!(&mut stderr, "{}", hint)?;
                }
                Ok(())
            }
            Error::Usage { details, requested } => {
                if *requested {
                    println!("{}", details);
                } else {
                    eprintln!("{}", details);
                }
                Ok(())
            }
        }
    }

    pub fn exit_code(&self) -> i32 {
        match self {
            // Requested usage details do not cause a failing exit code.
            Error::Usage {
                requested: true, ..
            } => 0,
            _ => 1,
        }
    }

    pub fn with_input_details(
        self,
        filename: &str,
        contents: &str,
        positioner: &Positioner,
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
            Error::General { cause, .. } => cause.fmt(f),
            Error::Usage { details, .. } => write!(f, "usage error: {}", details),
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

pub trait Positioner {
    fn line_col(&self, pos: usize) -> (usize, usize);
}

pub trait ResultExt<T, E> {
    fn err_ctx(self, ctx: String) -> Result<T, Error>
    where
        Self: Sized;
}

impl<T, E> ResultExt<T, E> for Result<T, E>
where
    E: 'static + StdError,
{
    fn err_ctx(self, ctx: String) -> Result<T, Error>
    where
        Self: Sized,
    {
        self.map_err(|err| Error::General {
            ctx,
            cause: Box::new(err),
            hints: Vec::new(),
        })
    }
}
