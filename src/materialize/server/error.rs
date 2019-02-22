// Copyright 2019 Timely Data, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Timely Data, Inc.

use std::fmt;
use std::io;

#[derive(Debug)]
pub enum Error {
    Http(hyper::Error),
    Io(io::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Error::Http(e) => write!(f, "http error: {}", e),
            Error::Io(e) => write!(f, "i/o error: {}", e),
        }
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Error::Io(err)
    }
}

impl From<hyper::Error> for Error {
    fn from(err: hyper::Error) -> Self {
        Error::Http(err)
    }
}
